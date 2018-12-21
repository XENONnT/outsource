
import subprocess
import os
import threading
import tempfile
import time


class Shell(object):
    '''
    Provides a shell callout with buffered stdout/stderr, error handling and timeout
    '''
        
    def __init__(self, cmd, timeout_secs = 1*60*60, log_cmd = False, log_outerr = False):
        self._cmd = cmd
        self._timeout_secs = timeout_secs
        self._log_cmd = log_cmd
        self._log_outerr = log_outerr
        self._process = None
        self._out_file = None
        self._outerr = ''
        self._duration = 0.0


    def run(self):
        def target():
                        
            self._process = subprocess.Popen(self._cmd, shell=True, 
                                             stdout=self._out_file, 
                                             stderr=subprocess.STDOUT,
                                             preexec_fn=os.setpgrp)
            self._process.communicate()

        if self._log_cmd:
            print(self._cmd)
                    
        # temp file for the stdout/stderr
        self._out_file = tempfile.TemporaryFile(prefix='outsource-', suffix='.out')
        
        ts_start = time.time()
        
        thread = threading.Thread(target=target)
        thread.start()

        thread.join(self._timeout_secs)
        if thread.isAlive():
            # do our best to kill the whole process group
            try:
                kill_cmd = 'kill -TERM -%d' %(os.getpgid(self._process.pid))
                kp = subprocess.Popen(kill_cmd, shell=True)
                kp.communicate()
                self._process.terminate()
            except:
                pass
            thread.join()
            # log the output
            self._out_file.seek(0)
            stdout = self._out_file.read().decode("utf-8").strip()
            if self._log_outerr and len(stdout) > 0:
                print(stdout)
            self._out_file.close()
            raise RuntimeError('Command timed out after %d seconds: %s' %(self._timeout_secs, self._cmd))
        
        self._duration = time.time() - ts_start
        
        # log the output
        self._out_file.seek(0) 
        self._outerr = self._out_file.read().decode("utf-8").strip()
        if self._log_outerr and len(self._outerr) > 0:
            print(self._outerr)
        self._out_file.close()
        
        if self._process.returncode != 0:
            raise RuntimeError('Command exited with non-zero exit code (%d): %s\n%s' \
                               %(self._process.returncode, self._cmd, self._outerr))

    def get_outerr(self):
        """
        returns the combined stdout and stderr from the command
        """
        return self._outerr
    
    
    def get_exit_code(self):
        """
        returns the exit code from the process
        """
        return self._process.returncode
    
    def get_duration(self):
        """
        returns the timing of the command (seconds)
        """
        return self._duration
