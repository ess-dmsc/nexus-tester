import h5py

def check_nexus_groups(file, errors):
    mandatory_groups = ['entry']
    for group in mandatory_groups:
        if group not in file:
            errors.append(f"Error: Mandatory group '{group}' not found.")
            return False
    return True

def check_nxlog_group(nxlog_group, errors):
    mandatory_fields = ['value', 'time']
    for field in mandatory_fields:
        if field not in nxlog_group:
            errors.append(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field.")
            continue

        if 'units' not in nxlog_group[field].attrs:
            errors.append(f"Error: '{field}' field in NXlog group '{nxlog_group.name}' must have 'units' attribute.")

        if len(nxlog_group[field]) < 1:
            errors.append(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field with a minimum length of 1.")

def recursive_check(group, errors):
    if group.attrs.get('NX_class') == 'NXlog':
        check_nxlog_group(group, errors)

    for item in group.values():
        if isinstance(item, h5py.Group):
            recursive_check(item, errors)

def check_entry_group(entry_group, errors):
    mandatory_attributes = ['NX_class']
    mandatory_fields = ['title', 'start_time', 'end_time', 'definition']

    for attr in mandatory_attributes:
        if attr not in entry_group.attrs:
            errors.append(f"Error: Mandatory attribute '{attr}' not found in entry group.")
            return

    if entry_group.attrs['NX_class'] != 'NXentry':
        errors.append("Error: Entry group NX_class should be 'NXentry'.")

    for field in mandatory_fields:
        if field not in entry_group:
            errors.append(f"Error: Mandatory field '{field}' not found in entry group.")

    recursive_check(entry_group, errors)

def check_nexus_file(file_path):
    errors = []
    with h5py.File(file_path, 'r') as file:
        check_nexus_groups(file, errors)
        entry_group = file.get('entry')
        if entry_group:
            check_entry_group(entry_group, errors)

    if errors:
        print("The following errors were found:")
        for error in errors:
            print(f" - {error}")
    else:
        print("File adheres to NeXus standards.")

if __name__ == '__main__':
    nexus_file_path = '/home/jonas/code/motion_test/038243_00010245.hdf'
    check_nexus_file(nexus_file_path)
