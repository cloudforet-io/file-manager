ROUTER = [
    {
        "router_path": "spaceone.file_manager.interface.rest.file:router",
        "router_options": {
            "prefix": "/files",
        },
    },
    {
        "router_path": "spaceone.file_manager.interface.rest.user_file:router",
        "router_options": {
            "prefix": "/files",
        },
    },
]
