import h5py
from graphite_pusher import GraphiteQueue
from graylog_pusher import send_message_to_graylog

GRAPHITE_HOST = "10.100.211.201"
GRAPHITE_PORT = 2003
FAILED_METRIC_NAME = "nexus_tester.failed_tests"
PASSED_METRIC_NAME = "nexus_tester.passed_tests"

graphite_queue = GraphiteQueue(GRAPHITE_HOST, GRAPHITE_PORT)

def register_message(message, message_list, level):
    message_list.append(message)
    send_message_to_graylog(message, level)

def check_mandatory_fields(group, mandatory_fields, errors, passed):
    for field in mandatory_fields:
        if field not in group:
            register_message(f"Error: {group.attrs.get('NX_class')} group '{group.name}' must have '{field}' field.", errors, 3)
        else:
            register_message(f"Passed: {group.attrs.get('NX_class')} group '{group.name}' has '{field}' field.", passed, 7)

def check_nxlog_fields(nxlog_group, errors, passed):
    check_mandatory_fields(nxlog_group, ['value', 'time'], errors, passed)

    for field in ['value', 'time']:
        if field in nxlog_group:
            if 'units' not in nxlog_group[field].attrs:
                register_message(f"Error: '{field}' field in NXlog group '{nxlog_group.name}' must have 'units' attribute.", errors, 3)
            else:
                register_message(f"Passed: '{field}' field in NXlog group '{nxlog_group.name}' has 'units' attribute.", passed, 7)

            if len(nxlog_group[field]) < 1:
                register_message(f"Error: NXlog group '{nxlog_group.name}' must have '{field}' field with a minimum length of 1.", errors, 3)
            else:
                register_message(f"Passed: NXlog group '{nxlog_group.name}' has '{field}' field with a minimum length of 1.", passed, 7)

def check_nxinstrument_fields(nxinstrument_group, errors, passed):
    check_mandatory_fields(nxinstrument_group, ['name'], errors, passed)

def recursive_check(group, errors, passed):
    nx_class_checks = {
        'NXlog': check_nxlog_fields,
        'NXinstrument': check_nxinstrument_fields,
    }

    check_func = nx_class_checks.get(group.attrs.get('NX_class'))
    if check_func:
        check_func(group, errors, passed)

    for item in group.values():
        if isinstance(item, h5py.Group):
            recursive_check(item, errors, passed)

def check_entry_group(entry_group, errors, passed):
    check_mandatory_fields(entry_group, ['title', 'start_time', 'end_time', 'definition'], errors, passed)

    if entry_group.attrs.get('NX_class') != 'NXentry':
        register_message("Error: Entry group NX_class should be 'NXentry'.", errors, 3)
    else:
        register_message("Passed: Entry group NX_class is 'NXentry'.", passed, 7)

    recursive_check(entry_group, errors, passed)

def check_nexus_file(file_path):
    errors = []
    passed = []
    with h5py.File(file_path, 'r') as file:
        if 'entry' not in file:
            register_message("Error: Mandatory group 'entry' not found.", errors, 3)
        else:
            register_message("Passed: Mandatory group 'entry' found.", passed, 7)
            entry_group = file.get('entry')
            if entry_group:
                check_entry_group(entry_group, errors, passed)

    if errors:
        graphite_queue.add_metric(FAILED_METRIC_NAME, len(errors))
        graphite_queue.add_metric(PASSED_METRIC_NAME, len(passed))
        print(f"The following {len(errors)} errors were found:")
    else:
        graphite_queue.add_metric(FAILED_METRIC_NAME, 0)
        graphite_queue.add_metric(PASSED_METRIC_NAME, len(passed))
        print("File adheres to NeXus standards.")

if __name__ == '__main__':
    nexus_file_path = '/home/jonas/code/motion_test/038243_00010245.hdf'
    check_nexus_file(nexus_file_path)