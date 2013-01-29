import importlib
import os
import sys


if __name__ == "__main__":
    
    config_module = sys.argv[1]
    settings = importlib.import_module('.'.join(['config',config_module]))

    query_name = sys.argv[2]

    query_url = sys.argv[3]

    query_string = sys.argv[4]

    query_module = importlib.import_module(settings.query_module_name)
    query_class = getattr(query_module, settings.query_class_name)
    query = query_class(query_url, settings.query_fields)
    
    dirbuild_module = importlib.import_module(settings.dirbuild_module_name)
    dirbuild_class = getattr(dirbuild_module, settings.dirbuild_class_name)
    builder = dirbuild_class(settings.target_dir, os.path.join(settings.link_dir, query_name))
    
    fs_handler_module = importlib.import_module(settings.fs_handler_module_name)
    fs_handler_class = getattr(fs_handler_module, settings.fs_handler_class_name)
    fs_handler = fs_handler_class()

    
    new_dir = os.path.join(settings.link_dir, query_name)    

    fs_handler.mkdir(new_dir)
    links = builder.associate(query.run_query(query_string))
    for link, target in links.items():
        fs_handler.symlink(target, link)

    # Requires elevation of privilige
#    motd = open("/etc/motd", 'a')
#    motd.write("Created symlinks in directory %s\n" % new_dir)
#    motd.close()
