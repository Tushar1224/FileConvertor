from app import process_files
import sys
import json

if __name__ == '__main__':  
    db_names = json.loads(sys.argv[1]) if len(sys.argv) > 1 else None    
    process_files(db_names)


# $Env:src)base_dir = 'py_src/data/retail_db'
# $Env:tgt_base_dir = 'py_src/data/retail_db_json'
# python main,.py '[\"orders\", \"order_items\"]'