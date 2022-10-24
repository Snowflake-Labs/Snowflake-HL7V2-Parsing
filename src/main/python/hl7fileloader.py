'''
    An utility script to help load an hl7 message into a table. This is used as part of demo, relating to parsing HL7
    messages using UDF.
'''
import _snowflake
import os ,sys ,json ,zipfile ,tarfile
import importlib.util

# We extract the hl7apy, from the stage  into the local /tmp folder
# Extract the hl7apy.zip into tmp folder and dynamically import
# '''
#  We cannot import the hl7apy modules, asis. This is due to internal logic
#  which reads local directory path during run time. PythonUDF does not decompress
#  the zip file at runtime. Hence we need to decompress into the /tmp directory 
#  and dynamically import the required module files.
# '''
PACKAGE_ZIP = 'hl7apy-1.3.4.tar.gz'
PACKAGE_FNAME = PACKAGE_ZIP.replace('.tar.gz', '')
IMPORT_DIR = sys._xoptions["snowflake_import_directory"]
TARGET_LIB_PATH = f'/tmp/hl7pyparser' + str(os.getpid())

# Extract the library
tf = tarfile.open(f'{IMPORT_DIR}{PACKAGE_ZIP}')
tf.extractall(f'{TARGET_LIB_PATH}')

#Add the extracted folder to sys path
sys.path.insert(0 ,TARGET_LIB_PATH )
sys.path.insert(0 ,f'{TARGET_LIB_PATH}/{PACKAGE_FNAME}' )

#Import should be done, only after inserting the target_lib_path into the path
from hl7apy import parser
from hl7apy.parser import parse_message
 
def read_msgs(p_hl7msgfile):
    msgs = []
    with _snowflake.open(p_hl7msgfile ) as f:
        row_idx = 0
        for line in f:
            if row_idx > 200:
                break
            try:
                raw_msg = line.decode('ascii').strip()

                if raw_msg is None:
                    continue

                if len(raw_msg) < 10:
                    continue

                row_idx = row_idx + 1
            
                s = raw_msg #.replace("\n", "\r")
                
                if s is None:
                    continue
                
                msgs.append( s )
                
            except Exception as e:
                pass
    return msgs

def main(session, p_hl7msgfile ,p_targettable):
    msgs = read_msgs(p_hl7msgfile)
    total_msgs = len(msgs)
    
    df = session.create_dataframe(msgs, schema=["raw_msg"])
    df.write.save_as_table(p_targettable, mode="overwrite")
    
    ret = {
        "rows": total_msgs
    }
    return ret