# Agritech Farm water management

# Conf - All configurations
# src - code base
    -> things - package contains IoT setup and simulator module
        --> ThingsSetup.py - for creating things and attaching the certs to it.
            ---> In case 'create_things_using_conf_mapping' set to True in config, then mapping specified using thing_map option will be used. 
            ---> else [agri_iot -> ss_create / sp_create] highlights how many things has to be created and respective certificates.
        --> __init__.py - used for simulator
    -> setup - package contains aws service which has to be created.
        --> __inti__.py - used to orchestrate setup process and driven using config.
    -> central_processing - package contains the analytical function.