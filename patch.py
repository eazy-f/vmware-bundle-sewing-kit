#!/usr/bin/env python2.7

from __future__ import print_function

import argparse
import tempfile
import os
import os.path
import sys
from StringIO import StringIO

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--extracted', help = 'extracted bundle contents directory')
    parser.add_argument('bundle', help = 'vmware installer bundle file')
    args = parser.parse_args()
    bundle_dir = get_bundle_dir(args.bundle, args.extracted)
    add_vmware_libraries(bundle_dir)
    patched_files = apply_bundle_patch(bundle_dir)
    original_descriptor = load_bundle_descriptor(args.bundle)
    patched_descriptor = patch_bundle_descriptor(original_descriptor, patched_files)
    print(generate_manifest(patched_descriptor))

def get_bundle_dir(bundle, extraced):
    if extraced:
        return extraced
    else:
        temp = os.path.join(tempfile.mkdtemp('', 'sewing-kit'), 'bundle')
        os.system('{} -x {} --console'.format(bundle, temp))
        return temp

def apply_bundle_patch(bundle_dir):
    vmmon = 'vmware-vmx/lib/modules/source/vmmon.tar'
    patched_files = []
    with tempfile.TemporaryFile() as patched:
        patched.write('ololo\n')
        patched_files.append(('vmware-vmx', vmmon, patched.name))
    return patched_files

def add_vmware_libraries(bundle_dir):
    sys.path.append(os.path.join(bundle_dir, 'vmware-installer'))

def load_bundle_descriptor(bundle_path):
    from vmis.core.bundle import Bundle
    from vmis.core.common import SetRepository
    SetRepository(None)
    with open(bundle_path, 'rb') as bundle:
        return Bundle.LoadBundle(bundle)

def patch_bundle_descriptor(original_descriptor, patched_files):
    file_offsets = {}
    patched_entries = get_patched_entries(original_descriptor.components, patched_files)
    for component in original_descriptor.components:
        for path, file_entry in component.fileset:
            file_offsets[file_entry.offset] = (component, file_entry)
            for patched_component, patched_file in patched_entries:
                if patched_component == component and patched_file.path == file_entry.path:
                    del file_offsets[file_entry.offset]
    
    manifest_shift = 0
    moving_files = patched_entries
    relocated_files = []
    relocated_offset = 0
    remaining_offsets = sorted(file_offsets.keys())
    for component, moving in moving_files:
        file_descriptor_shift = estimated_manifest_shift(moving, relocated_offset)
        manifest_shift = manifest_shift + file_descriptor_shift
        is_affected = lambda offset: offset < manifest_shift
        affected_offsets = filter(is_affected, remaining_offsets)
        affected_files = [file_offsets[offset] for offset in affected_offsets]
        remaining_offsets = remaining_offsets[len(affected_offsets):]
        moving_files.extend(affected_files)
        relocated_files.append((component, relocate(moving, relocated_offset)))
        relocated_offset = relocated_offset + moving.compressedSize
    new_bundle = update_bundle(original_descriptor, relocated_files)
    return new_bundle

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

def relocate(file_entry, target_offset):
    if file_entry is PatchedFile:
        file_entry.offset = target_offset
        return file_entry
    else:
        return RelocatedFileEntry(file_entry, target_offset)

class RelocatedFileEntry(object):
    def __init__(self, original_entry, target_offset):
        self.original_entry = original_entry
        self.offset = target_offset
        self.path = self.original_entry.path
        self.compressedSize = self.original_entry.compressedSize
        self.uncompressedSize = self.original_entry.uncompressedSize

    def __repr__(self):
        return str({'offset': self.offset, 'path': self.original_entry.path})

class PatchedFile(object):
    def __init__(self, path, uncompressedSize, compressedSize, content):
        self.path = path
        self.uncompressedSize = uncompressedSize
        self.compressedSize = compressedSize
        self.offset = 0

    @staticmethod
    def create_from_file(path, content):
        return PatchedFile(path, 100, 100, StringIO('hello'))

def get_patched_entries(components, patched_files):
    component_map = dict([(c.name, c) for c in components])
    return [(component_map[component], PatchedFile.create_from_file(path, fs_path))
             for component, path, fs_path in patched_files]

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
    components = etree.SubElement(product, 'components')
    for component in bundle.components:
        xml_component = etree.SubElement(components, 'component')

    print(bundle.productComponents)
    return etree.tostring(root, pretty_print = True)

if __name__ == '__main__':
    main()
