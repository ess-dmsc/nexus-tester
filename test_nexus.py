import h5py
from graphite_pusher import send_metric_to_graphite

GRAPHITE_HOST = "10.100.211.201"
GRAPHITE_PORT = 2003
FAILED_METRIC_NAME = "nexus_tester.failed_tests"
PASSED_METRIC_NAME = "nexus_tester.passed_tests"

def check_nexus_groups(file, errors):
    mandatory_groups = ['entry']
    for group in mandatory_groups:
        if group not in file:
            errors.append(f"Error: Mandatory group '{group}' not found.")
            return False
    return True

def check_nxlog_group(nxlog_group, errors, passed):
    mandatory_fields = ['value', 'time']
    for field in mandatory_fields:
        if field not in nxlog_group:
            errors.append(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field.")
            continue
        else:
            passed.append(f"Passed: NXlog group '{nxlog_group.name}' has '{field}' field.")

        if 'units' not in nxlog_group[field].attrs:
            errors.append(f"Error: '{field}' field in NXlog group '{nxlog_group.name}' must have 'units' attribute.")
        else:
            passed.append(f"Passed: '{field}' field in NXlog group '{nxlog_group.name}' has 'units' attribute.")

        if len(nxlog_group[field]) < 1:
            errors.append(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field with a minimum length of 1.")
        else:
            passed.append(f"Passed: NXlog group '{nxlog_group.name}' has '{field}' field with a minimum length of 1.")

def recursive_check(group, errors, passed):
    if group.attrs.get('NX_class') == 'NXlog':
        check_nxlog_group(group, errors, passed)

    for item in group.values():
        if isinstance(item, h5py.Group):
            recursive_check(item, errors, passed)

def check_entry_group(entry_group, errors, passed):
    mandatory_attributes = ['NX_class']
    mandatory_fields = ['title', 'start_time', 'end_time', 'definition']

    for attr in mandatory_attributes:
        if attr not in entry_group.attrs:
            errors.append(f"Error: Mandatory attribute '{attr}' not found in entry group.")
            return
        else:
            passed.append(f"Passed: Mandatory attribute '{attr}' found in entry group.")

    if entry_group.attrs['NX_class'] != 'NXentry':
        errors.append("Error: Entry group NX_class should be 'NXentry'.")
    else:
        passed.append("Passed: Entry group NX_class is 'NXentry'.")

    for field in mandatory_fields:
        if field not in entry_group:
            errors.append(f"Error: Mandatory field '{field}' not found in entry group.")
        else:
            passed.append(f"Passed: Mandatory field '{field}' found in entry group.")

    recursive_check(entry_group, errors, passed)

def check_nexus_file(file_path):
    errors = []
    passed = []
    with h5py.File(file_path, 'r') as file:
        check_nexus_groups(file, errors)
        if not errors:
            passed.append("Mandatory group 'entry' found.")
        entry_group = file.get('entry')
        if entry_group:
            check_entry_group(entry_group, errors, passed)

    if errors:
        print("sending errors")
        send_metric_to_graphite(GRAPHITE_HOST, GRAPHITE_PORT, FAILED_METRIC_NAME, len(errors))
        print("sending passed")
        send_metric_to_graphite(GRAPHITE_HOST, GRAPHITE_PORT, PASSED_METRIC_NAME, len(passed))
        print(f"The following {len(errors)} errors were found:")
        # for error in errors:
        #     print(f" - {error}")
    else:
        send_metric_to_graphite(GRAPHITE_HOST, GRAPHITE_PORT, FAILED_METRIC_NAME, 0)
        send_metric_to_graphite(GRAPHITE_HOST, GRAPHITE_PORT, PASSED_METRIC_NAME, len(passed))
        print("File adheres to NeXus standards.")

if __name__ == '__main__':
    nexus_file_path = '/home/jonas/code/motion_test/038243_00010245.hdf'
    check_nexus_file(nexus_file_path)
