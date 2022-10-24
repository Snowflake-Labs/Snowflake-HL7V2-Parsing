"""
Used to parse HL7 v2.x messages. The implementation uses the hl7apy library.
The variable PACKAGE_ZIP has to be updated, if a different file name is used.
"""
import os
import sys
import tarfile

# Extract the hl7apy.zip into tmp folder and dynamically import
"""
 We cannot import the hl7apy modules, asis. This is due to internal logic
 which reads local directory path during run time. PythonUDF does not decompress
 the zip file at runtime. Hence we need to decompress into the /tmp directory 
 and dynamically import the required module files.
"""
PACKAGE_ZIP = 'hl7apy-1.3.4.tar.gz'
PACKAGE_FNAME = PACKAGE_ZIP.replace('.tar.gz', '')
IMPORT_DIR = sys._xoptions["snowflake_import_directory"]
TARGET_LIB_PATH = f'/tmp/hl7pyparser' + str(os.getpid())

# Extract the library
tf = tarfile.open(f'{IMPORT_DIR}{PACKAGE_ZIP}')
tf.extractall(f'{TARGET_LIB_PATH}')

# Add the extracted folder to sys path
sys.path.insert(0, TARGET_LIB_PATH)
sys.path.insert(0, f'{TARGET_LIB_PATH}/{PACKAGE_FNAME}')

# Import should be done, only after inserting the target_lib_path into the path
from hl7apy.parser import parse_message


def hl7messageobj_to_dict(m, p_use_long_name=True):
    """Convert an HL7 message to a dictionary
    :param m: The HL7 message
    :param p_use_long_name: Whether or not to user the long names
                          (e.g. "patient_name" instead of "pid_5")
    :returns: A dictionary representation of the HL7 message
    """
    if m.children:
        d = {}
        for c in m.children:
            name = str(c.name).lower()
            if p_use_long_name:
                name = str(c.long_name).lower() if c.long_name else name
            dictified = hl7messageobj_to_dict(c, p_use_long_name=p_use_long_name)
            if name in d:
                if not isinstance(d[name], list):
                    d[name] = [d[name]]
                d[name].append(dictified)
            else:
                d[name] = dictified
        return d
    else:
        return m.to_er7()


def udf(p_hl7v2_raw_msg):
    """Convert HL7 string to json
    """

    # replace new line with carriage return
    s = p_hl7v2_raw_msg.replace("\n", "\r")

    # Parse the message into object
    hl7_obj = parse_message(s)

    hl7_dict = hl7messageobj_to_dict(hl7_obj, True)
    return hl7_dict
