import h5py
from graphite_pusher import GraphiteQueue
from graylog_pusher import send_message_to_graylog

GRAPHITE_HOST = "10.100.211.201"
GRAPHITE_PORT = 2003
FAILED_METRIC_NAME = "nexus_tester.failed_tests"
PASSED_METRIC_NAME = "nexus_tester.passed_tests"

graphite_queue = GraphiteQueue(GRAPHITE_HOST, GRAPHITE_PORT)


def register_error(error_message, error_list):
    error_list.append(error_message)
    send_message_to_graylog(error_message, 3)


def register_pass(pass_message, pass_list):
    pass_list.append(pass_message)
    send_message_to_graylog(pass_message, 2)


def check_nexus_groups(file, errors, passed):
    mandatory_groups = ['entry']
    for group in mandatory_groups:
        if group not in file:
            register_error(f"Error: Mandatory group '{group}' not found.", errors)
            return False
        else:
            register_pass(f"Passed: Mandatory group '{group}' found.", passed)
    return True

def check_nxlog_group(nxlog_group, errors, passed):
    mandatory_fields = ['value', 'time']
    for field in mandatory_fields:
        if field not in nxlog_group:
            register_error(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field.", errors)
            continue
        else:
            register_pass(f"Passed: NXlog group '{nxlog_group.name}' has '{field}' field.", passed)

        if 'units' not in nxlog_group[field].attrs:
            register_error(f"Error: '{field}' field in NXlog group '{nxlog_group.name}' must have 'units' attribute.", errors)
        else:
            register_pass(f"Passed: '{field}' field in NXlog group '{nxlog_group.name}' has 'units' attribute.", passed)

        if len(nxlog_group[field]) < 1:
            register_error(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field with a minimum length of 1.", errors)
        else:
            register_pass(f"Passed: NXlog group '{nxlog_group.name}' has '{field}' field with a minimum length of 1.", passed)

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
            register_error(f"Error: Mandatory attribute '{attr}' not found in entry group.", errors)
            return
        else:
            register_pass(f"Passed: Mandatory attribute '{attr}' found in entry group.", passed)

    if entry_group.attrs['NX_class'] != 'NXentry':
        register_error("Error: Entry group NX_class should be 'NXentry'.", errors)
    else:
        register_pass("Passed: Entry group NX_class is 'NXentry'.", passed)

    for field in mandatory_fields:
        if field not in entry_group:
            register_error(f"Error: Mandatory field '{field}' not found in entry group.", errors)
        else:
            register_pass(f"Passed: Mandatory field '{field}' found in entry group.", passed)

    recursive_check(entry_group, errors, passed)

def check_nexus_file(file_path):
    errors = []
    passed = []
    with h5py.File(file_path, 'r') as file:
        check_nexus_groups(file, errors, passed)
        if not errors:
            register_pass("Mandatory group 'entry' found.", errors)
        entry_group = file.get('entry')
        if entry_group:
            check_entry_group(entry_group, errors, passed)

    if errors:
        graphite_queue.add_metric(FAILED_METRIC_NAME, len(errors))
        graphite_queue.add_metric(PASSED_METRIC_NAME, len(passed))
        print(f"The following {len(errors)} errors were found:")
        # for error in errors:
        #     print(f" - {error}")
    else:
        graphite_queue.add_metric(FAILED_METRIC_NAME, 0)
        graphite_queue.add_metric(PASSED_METRIC_NAME, len(passed))
        print("File adheres to NeXus standards.", errors)

if __name__ == '__main__':
    nexus_file_path = '/home/jonas/code/motion_test/038243_00010245.hdf'
    check_nexus_file(nexus_file_path)
