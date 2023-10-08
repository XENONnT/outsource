# log_digest

written il perl, log_digest parsing input files returns in short any error message characterized by the word ERROR in it. 

### usage 

if executable (remember to chmod 755 if not), just use the following command inside the exe directory

```./log_digest.pl file1 file2 --error_list commomn_error_patterns.txt```

It can process multiple files at the same time. If commomn_error_patterns.txt is not provided, it searches for the key-word **ERROR**. 
The file commomn_error_patterns.txt has to provide for each line a patter to be searched such as:
```
Stale file handle: ‘/tmp’
Cannot merge chunks with different number of items
strax.storage.common.DataCorrupted
```

### example 

```./log_digest.pl file* ```

this command line produces the following output:

```
Digesting: file1.txt file2.txt

======================= Output for: file1.txt =======================
Error pattern stats:
-----------------------------------------------------------------
- ERROR: found 34 times in the logfile
-----------------------------------------------------------------
Error msg: ERROR  ConnectionError: (, RemoteDisconnected())      in lines [39285, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url:    in lines [50839, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url:    in lines [41061, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url:    in lines [41058, ]
Error msg: ERROR:  Command exited with non-zero exit code (11):         in lines [45491, 45504, 45517, 45530, 45543, 46227, 46240, 46253, 46266, 46279, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: (Caused by NewConnectionError())          in lines [44246, ]

======================= Output for: file2.txt =======================
Error pattern stats:
-----------------------------------------------------------------
- ERROR: found 3 times in the logfile
-----------------------------------------------------------------
Error msg: ERROR - Failed to download file       in lines [53040, ]

Execution time: 0.07 s
```
