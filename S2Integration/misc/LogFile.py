class LogFile:
    """Class to manage the log file for the batch processing of parquet files"""

    def __init__(self, log_file=None):
        """Initialise the log file"""
        self.log_file = log_file
        self.logging_idx = self._read_log_idx()

    def _read_log_idx(self):
        """Read the log index from the log file"""
        try:
            with open(self.log_file, 'r') as f:
                return int(f.read())
        except FileNotFoundError:
            return 0

    def _write_log_idx(self):
        """Write the log index to the log file"""
        with open(self.log_file, 'w') as f:
            f.write(str(self.logging_idx))

    def get_log_idx(self):
        """Return the current log index"""
        return self.logging_idx

    def update_log_idx(self, idx):
        """Update the log index and write to the log file"""
        self.logging_idx = idx
        self._write_log_idx()

