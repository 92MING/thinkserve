import sys
import argparse

from pathlib import Path

try:
    from thinkserve.service.manager import ServiceManager
    from thinkserve.service.configs import ServiceConfigs
except ImportError as e:
    thinkserve_path = Path(__file__).parent.parent.parent
    if str(thinkserve_path) not in sys.path:
        sys.path.append(str(thinkserve_path))
    from thinkserve.service.manager import ServiceManager
    from thinkserve.service.configs import ServiceConfigs

def main():
    parser = argparse.ArgumentParser(description="Start a ThinkServe service.")
    parser.add_argument('service_config_path', type=str, help='Path to the service configuration file.')
    args = parser.parse_args()
    
    service_config_path = args.service_config_path
    if not service_config_path:
        print("Error: No service configuration file path provided.")
        exit(1)
    config_path = Path(service_config_path)
    if config_path.is_dir():
        maybe_service_config_path = config_path / 'service_config.json'
        if maybe_service_config_path.exists():
            service_config_path = maybe_service_config_path
        else:
            print(f"Error: No service configuration file found in the specified directory: {service_config_path}")
            exit(1)
    if not config_path.is_file():
        print(f"Error: The specified service configuration file does not exist: {service_config_path}")
        exit(1)
    try:
        service_configs = ServiceConfigs.model_validate_json(config_path.read_text())
    except Exception as e:
        print(f"Error: Failed to load service configuration from {service_config_path}: {e}")
        exit(1)
    manager = ServiceManager(configs=service_configs, service=service_configs.get_service_type())
    manager.start()
    while True:
        try:
            pass
        except KeyboardInterrupt:
            print("Shutting down the service...")
            manager.stop()
            break
    
    
if __name__ == '__main__':
    main()