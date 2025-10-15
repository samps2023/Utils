# import logging
# from logging.handlers import TimedRotatingFileHandler
# import datetime
# import os


# def setup_logger(save_log: bool = True,new_file_per_run: bool = True,log_name="application"):
#     # Get current timestamp
#     timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

#     # Create logger
#     logger = logging.getLogger("my_logger")
#     logger.setLevel(logging.DEBUG)

#     # Create formatter
#     formatter = logging.Formatter(
#         "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
#     )

#     if save_log:
#         #Ensure logs directory exists
#         log_dir ="./logs"
#         os.makedirs(log_dir,exist_ok=True)

#         if new_file_per_run:
#             log_file = os.path.join(log_dir, f"{timestamp}.log")  # New file per run
#         else:
#             log_file = os.path.join(log_dir, f"{log_name}.log")  # Append to the same file

#         # Create rotating file handler with timestamp in filename
#         file_handler = TimedRotatingFileHandler(
#             f"./logs/{timestamp}.log", when="midnight", interval=1, backupCount=7, encoding='utf-8'
#         )
#         file_handler.setLevel(logging.DEBUG)
#         file_handler.setFormatter(formatter)

#         # Add file handler to logger
#         logger.addHandler(file_handler)

#         logging.captureWarnings(True)

#     return logger

import logging
import datetime
import os
from logging.handlers import TimedRotatingFileHandler

class CustomLogger(logging.Logger):
    def __init__(self, name, log_file,new_file_per_run, level=logging.DEBUG):
        super().__init__(name, level)
        self.start_times = {}  # Store start times for different tasks
        
        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # Choose between TimedRotatingFileHandler (new file per run) or FileHandler (append)
        if new_file_per_run:
            self.file_handler = TimedRotatingFileHandler(
                log_file, when="midnight", interval=1, backupCount=7, encoding='utf-8'
            )
        else:
            self.file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(formatter)

        self.addHandler(self.file_handler)

    def start(self, name: str):
        """Log the start of a process and store the start time."""
        start_time = datetime.datetime.now()
        self.start_times[name] = start_time
        self.info(f"### Start {name}: {start_time}")

    def end(self, name: str):
        """Log the end of a process and calculate duration."""
        end_time = datetime.datetime.now()
        start_time = self.start_times.get(name)
        if start_time:
            duration = end_time - start_time
            self.info(f"### Complete {name}: {end_time}, Duration: {duration}")
        else:
            self.warning(f"### End {name}: No start time found!")

    def add_separator(self, length=120):
        """Add a separator of a specified length without the log prefix."""
        # Temporarily log directly without any log level/timestamp, just a raw separator line
        with open(self.file_handler.baseFilename, 'a', encoding='utf-8') as f:
            f.write("-" * length + "\n")

def setup_logger(save_log: bool = True, new_file_per_run: bool = False,log_name="application"):
    """Setup logger with optional file rotation and log helpers for start/end tracking."""
    
    # Get current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Define log directory
    log_dir = "./logs"
    os.makedirs(log_dir, exist_ok=True)  # Ensure logs directory exists

    # Choose log filename
    log_file = os.path.join(log_dir, f"{timestamp}.log") if new_file_per_run else os.path.join(log_dir, f"{log_name}.log")

    # Create a custom logger instance
    logger = CustomLogger("my_logger", log_file, new_file_per_run)

    logging.captureWarnings(True)  # Capture warnings

    if not new_file_per_run:
        logger.add_separator()

    return logger

def run_step(logger, step_name, func):
    """Runs a step with error handling and logging. Stops execution on failure."""
    try:
        logger.start(step_name)
        result = func()  # Run the actual step function
        logger.end(step_name)
        return result
    except Exception as e:
        logger.error(f"{step_name} failed: {e}")
        raise SystemExit(f"Stopping execution due to {step_name} failure.")
