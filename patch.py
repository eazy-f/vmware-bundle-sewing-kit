#!/usr/bin/env python2.7

from __future__ import print_function

import argparse
import tempfile
import os
import os.path
import sys
import struct
import io
from gzip import GzipFile
from StringIO import StringIO
from operator import itemgetter, attrgetter
from binascii import crc32

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--extracted', help = 'extracted bundle contents directory')
    parser.add_argument('--patch', help = '<component> <object> <patched_file>', nargs=3, action = 'append', dest = 'patches')
    parser.add_argument('bundle', help = 'vmware installer bundle file')
    args = parser.parse_args()
    bundle_dir = get_bundle_dir(args.bundle, args.extracted)
    add_vmware_libraries(bundle_dir)
##    patched_files = apply_bundle_patch(bundle_dir)
    patched_files = args_patch(args.patches)
    with open(args.bundle, 'rb') as bundle:
        original_descriptor = load_bundle_descriptor(bundle)
        patched_descriptor = patch_bundle_descriptor(original_descriptor, patched_files)
        get_file_bundle(patched_descriptor).patch(args.bundle, '/tmp/file1')

def get_bundle_dir(bundle, extraced):
    if extraced:
        return extraced
    else:
        temp = os.path.join(tempfile.mkdtemp('', 'sewing-kit'), 'bundle')
        os.system('{} -x {} --console'.format(bundle, temp))
        return temp

def apply_bundle_patch(bundle_dir):
    vmmon = 'lib/modules/source/vmmon.tar'
    patched_files = []
    with tempfile.TemporaryFile() as patched:
        patched.write('ololo\n')
        patched_files.append(('vmware-vmx', vmmon, patched.name))
    return patched_files

def args_patch(patches):
    return patches

def add_vmware_libraries(bundle_dir):
    sys.path.append(os.path.join(bundle_dir, 'vmware-installer'))

def load_bundle_descriptor(bundle):
    from vmis.core.bundle import Bundle
    from vmis.core.common import SetRepository
    SetRepository(None)
    return Bundle.LoadBundle(bundle)

def patch_bundle_descriptor(original_descriptor, patched_files):
    from vmis.core.component import FileSet, FileEntry, Component
    from vmis.core.bundle import Bundle
    patched_entries = get_patched_entries(patched_files)
    patched_components = []
    for component in original_descriptor.components:
        patched_fileset = None
        if component.fileset:
            _paths, entries = zip(*component.fileset)
            patched_fileset = FileSet()
            offset_diff = 0
            for entry in sorted(entries, key = attrgetter('offset')):
                path = entry.path
                if path in patched_entries.get(component.name, {}):
                    patched_entry = patched_entries[component.name][path]
                    size_diff = patched_entry.compressedSize - entry.compressedSize
                    patched_entry.offset = entry.offset + offset_diff
                    offset_diff = offset_diff + size_diff
                    patched_fileset[path] = patched_entry
                else:
                    patched_fileset[path] = FileEntry(entry.path,
                                                      entry.uncompressedSize,
                                                      entry.compressedSize,
                                                      entry.offset + offset_diff)
        patched_components.append(copy_component(component, patched_fileset))
    return copy_bundle(original_descriptor, patched_components)

def estimated_manifest_shift(file_entry, relocated_offset):
    size_change = get_size_change(file_entry)
    offset_change = base10_diff(relocated_offset, file_entry.offset)
    return size_change + base10_len(offset_change)

def base10_len(digit):
    return len(str(digit))

def base10_diff(b, a):
    return base10_len(b) - base10_len(a)

def get_size_change(file_entry):
    if file_entry is PatchedFile:
        return base10_diff(file_entry.size, file_entry.original.size)
    else:
        return 0

class PatchedFile(object):
    def __init__(self, path, uncompressedSize, content):
        self.path = path
        self.uncompressedSize = uncompressedSize
        self.compressedSize = len(content.getvalue())
        self.offset = 0
        self.content = content

    @staticmethod
    def create_from_file(path, fs_path):
        content = io.BytesIO()
        with open(fs_path, 'rb') as source:
            destination = GzipFile(fileobj = content, mode = 'wb')
            size = copy_data(source, destination)
            destination.close()
            return PatchedFile(path, size, content)

def get_patched_entries(patched_files):
    entries_map = {}
    for component_name, path, fs_path in patched_files:
        entries = entries_map.get(component_name, {})
        entries[path] = PatchedFile.create_from_file(path, fs_path)
        entries_map[component_name] = entries

    return entries_map

def update_bundle(bundle, changed_files):
    from vmis.core.component import Component, FileSet, FileEntry
    from vmis.core.bundle import Bundle
    updated_components = []
    for component in bundle.components:
        updated_fileset = None
        if component.fileset:
            files = [(f.path, patched_file_entry(f)) for c, f in changed_files
                                                     if c == component]
            updated_fileset = dict(files)
            for path, e in component.fileset.items():
                if path not in updated_fileset:
                    updated_fileset[path] = FileEntry(path, e.uncompressedSize,
                                                      e.compressedSize, e.offset)
        updated_component = Component(component.name,
                                      component.longName,
                                      component.version,
                                      component.buildNumber,
                                      component.description,
                                      component.platform,
                                      component.architecture,
                                      component.coreVersion,
                                      component.dependencies,
                                      component.conflicts,
                                      component.optionalDependencies,
                                      component.reverseDependencies,
                                      component.eula,
                                      updated_fileset,
                                      component.local)
        updated_components.append(updated_component)
    updated = Bundle(bundle.coreVersion, updated_components, bundle.productComponents)
    return updated

def patched_file_entry(patched_file_entry):
    from vmis.core.component import FileEntry
    e = patched_file_entry
    return FileEntry(e.path, e.uncompressedSize, e.compressedSize, e.offset)

def generate_manifest(bundle):
    from lxml import etree
    root = etree.Element('bundle')
    product = etree.SubElement(root, 'product')
    coreVersion = etree.SubElement(product, 'coreVersion')
    coreVersion.text = bundle.coreVersion
    components = etree.SubElement(root, 'components')
    productComponents = etree.SubElement(product, 'components')
    for component in bundle.productComponents:
        xml_component = etree.SubElement(productComponents, 'component', ref=component)
    for component in bundle.components:
        xml_component = etree.SubElement(components, 'component',
                                         name = component.name,
                                         offset = str(bundle.componentLocation(component)),
                                         size = str(component.size))
    return etree.tostring(root, pretty_print = True)

def copy_component(component, new_fileset):
    from vmis.core.component import Component
    return copy_generic_component(Component, component, new_fileset)

def copy_generic_component(cls, component, new_fileset):
    copy = cls(component.name,
               component.longName,
               component.version,
               component.buildNumber,
               component.description,
               component.platform,
               component.architecture,
               component.coreVersion,
               component.dependencies,
               component.conflicts,
               component.optionalDependencies,
               component.reverseDependencies,
               component.eula,
               new_fileset,
               component.local)
    copy.original = component
    return copy

def copy_bundle(bundle, new_components):
    from vmis.core.bundle import Bundle
    return copy_generic_bundle(Bundle, bundle, new_components)

def copy_generic_bundle(cls, bundle, new_components):
    return cls(bundle.coreVersion, new_components, bundle.productComponents)

def get_file_bundle(bundle):
    from vmis.core.component import Component, \
                                    FileComponent as ReadFileComponent, \
                                    ComponentFileObj
    from vmis.core.bundle import Bundle
    class FileComponent(ReadFileComponent):
        def __init__(self, *args):
            super(FileComponent, self).__init__(*args)
            self.fileset_size = sum(map(attrgetter('compressedSize'),
                                        self.fileset.values()))
            self.manifest = self.GenerateManifestDoc(self)
            self.size = self.fileset_size + \
                        len(self.manifest) + ReadFileComponent.HEADER_SIZE

        def fileWrite(self, destination):
            manifest_offset = ReadFileComponent.HEADER_SIZE
            version = '1'
            header2_fields = [0, 0, version, manifest_offset,
                              len(self.manifest), len(self.manifest) + manifest_offset,
                              self.fileset_size]
            header2 = struct.pack(ReadFileComponent.HEADER_FORMAT, *header2_fields)
            checksum = crc32(header2[8:])
            header1_fields = [ReadFileComponent.MAGIC_NUMBER, checksum, version] + ([0] * 4)
            header1 = struct.pack(ReadFileComponent.HEADER_FORMAT, *header1_fields)
            destination.write(header1[:8] + header2[8:])
            destination.write(self.manifest)
            for entry in sorted(self.fileset.values(), key = attrgetter('offset')):
                start = destination.tell()
                copy_data(self.GetFile(entry), destination)
                end = destination.tell()
                assert end - start == entry.compressedSize, '{}: {} {} {}'.format(entry.path, (end - start), entry.compressedSize, entry)

        def GetFile(self, entry):
            if isinstance(entry, PatchedFile):
                return io.BytesIO(entry.content.getvalue())
            else:
                source_component = self.original.original
                source_entry = source_component.fileset[entry.path]
                start = source_component.dataOffset + source_entry.offset
                end = start + source_entry.compressedSize - 1
                content = ComponentFileObj(source_component.source, start, end).read()
                return io.BytesIO(content)

        @classmethod
        def GenerateManifestDoc(cls, component):
            from lxml import etree
            root = etree.Element('component')
            properties = component.manifestDict.copy()
            properties['coreVersion'] = component.coreVersion
            for name, value in properties.items():
                etree.SubElement(root, name).text=value
            if component.eula:
                etree.SubElement(root, 'eula').text = component.eula
            dependencies = etree.SubElement(root, 'dependencies')
            for dep in component.dependencies:
                optional = dep.optional
                dep.optional = False
                etree.SubElement(dependencies, 'dependency',
                                 name = str(dep), optional = str(optional).lower())
                dep.optional = optional
            fileset = etree.SubElement(root, 'fileset')
            for entry in component.fileset.values():
                props = {
                    'path': entry.path,
                    'compressedSize': str(entry.compressedSize),
                    'uncompressedSize': str(entry.uncompressedSize),
                    'offset': str(entry.offset)
                }
                etree.SubElement(fileset, 'file', **props)
            return etree.tostring(root)

        @staticmethod
        def Create(component):
            return copy_generic_component(FileComponent, component, component.fileset)

    class FileBundle(Bundle):
        def __init__(self, *args):
            super(FileBundle, self).__init__(*args)
            self.locations = self.ArrangeLocations(self.components)

        def componentLocation(self, component):
            return self.locations[component]

        def patch(self, from_path, to_path):
            with open(from_path, 'rb') as source:
                source.seek(-Bundle.FOOTER_SIZE, 2)
                source_footer = source.read(Bundle.FOOTER_SIZE)
                _, _, _, source_manifest_offset, payload_size, payload_offset, \
                    launcher_size, presize, preoffset, version, _, _ = \
                    struct.unpack(Bundle.FOOTER_FORMAT, source_footer)
                source.seek(0)
                with open(to_path, 'wb') as destination:
                    copy_data(source, destination, source_manifest_offset)
                    manifest = self.GenerateManifestDoc(self)
                    destination.write(manifest)
                    data_offset = destination.tell()
                    data_size = 0
                    arranged_components = sorted(self.locations.items(),
                                                 key = itemgetter(1))
                    for component, offset in arranged_components:
                        component.fileWrite(destination)
                    data_size = destination.tell() - data_offset
                    self.WriteFooter(destination, data_size, data_offset, len(manifest),
                                     source_manifest_offset, payload_size, payload_offset,
                                     launcher_size, presize, preoffset, version)

        @classmethod
        def ArrangeLocations(cls, components):
            locations = {}
            offset = 0
            for c in components:
                locations[c] = offset
                offset = offset + c.size
            return locations

        @classmethod
        def GenerateManifestDoc(cls, bundle):
            from lxml import etree
            root = etree.Element('bundle')
            product = etree.SubElement(root, 'product')
            coreVersion = etree.SubElement(product, 'coreVersion')
            coreVersion.text = bundle.coreVersion
            components = etree.SubElement(root, 'components')
            productComponents = etree.SubElement(product, 'components')
            for component in bundle.productComponents:
                xml_component = etree.SubElement(productComponents, 'component', ref=component)
            for component in bundle.components:
                xml_component = etree.SubElement(components, 'component',
                                                 name = component.name,
                                                 offset = str(bundle.locations[component]),
                                                 size = str(component.size))
            return etree.tostring(root)

        @staticmethod
        def WriteFooter(destination, dataSize, dataOffset, manifestSize,
                        manifestOffset, payloadSize, payloadOffset, launcherSize,
                        presize, preoffset, version):
            footer1 = struct.pack(Bundle.FOOTER_FORMAT, dataSize, dataOffset,
                                  manifestSize, manifestOffset, payloadSize,
                                  payloadOffset, launcherSize, presize,
                                  preoffset, version, 0, 0)
            checksum = Bundle.CalculateChecksum(footer1[:-8])
            footer2_fields = [0]*10 + [checksum, Bundle.MAGIC_NUMBER]
            footer2 = struct.pack(Bundle.FOOTER_FORMAT, *footer2_fields)
            destination.write(footer1[:-8] + footer2[-8:])

        @staticmethod
        def Create(bundle):
            file_components = [FileComponent.Create(c) for c in bundle.components]
            return copy_generic_bundle(FileBundle, bundle, file_components)

    return FileBundle.Create(bundle)

def copy_data(source, destination, size = None):
    bsize = 1024 * 1024
    b = bytearray('a'*bsize)
    write_bytes = True
    written = 0
    while write_bytes:
        read_bytes = source.readinto(b)
        if size != None:
            write_bytes = min(size, read_bytes)
            size = size - write_bytes
        else:
            write_bytes = read_bytes
        destination.write(bytes(b[0:write_bytes]))
        written = written + write_bytes
    return written

if __name__ == '__main__':
    main()
