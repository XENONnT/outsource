# log_digest

written il perl, log_digest parsing input files returns in short any error message characterized by the word ERROR in it. 

### usage 

if executable (remember to chmod 755 if not), just use the following command inside the exe directory

```./log_digest.pl file1 file2```

It can process multiple files at the same time. 

### example 

```./log_digest.pl input_files/pa_sr0_rn220_all_0.txt input_files/sr0_ambe_all_0.txt input_files/sr0_bkg_batch_0_0.txt input_files/sr0_ybe_all_0.txt ```

this command line produces the following output:

```
Digesting: input_files/pa_sr0_rn220_all_0.txt input_files/sr0_ambe_all_0.txt input_files/sr0_bkg_batch_0_0.txt input_files/sr0_ybe_all_0.txt

========= Output for: input_files/sr0_ambe_all_0.txt =========
Error msg: ERROR:  Command exited with non-zero exit code (1): /usr/bin/stashcp          in lines [38432, 38444, 38456, 38468, 38480, ]

========= Output for: input_files/sr0_ybe_all_0.txt =========
Error msg: ERROR - Failed to download file xnt_047876:peaklets-ui5hguaz2k-metadata.json          in lines [53040, ]

========= Output for: input_files/pa_sr0_rn220_all_0.txt =========
Error msg: ERROR:  Command exited with non-zero exit code (1): /usr/bin/stashcp          in lines [1000, 10006, 10018, 10030, 1012, 10207, 10219, 10231, 1024, 10243, 10255, 1036, 106, 10723, 10735, 10747, 10759, 10771, 10948, 10960, 10972, 10984, 10996, 11173, 11185, 11197, 11209, 11221, 11398, 11410, 11422, 11434, 11446, 11623, 11635, 11647, 11659, 11671, 118, 130, 13189, 13201, 13213, 13225, 13237, 13414, 13426, 13438, 13450, 13462, 141, 14751, 14763, 14775, 14787, 14799, 152, 15599, 15612, 15624, 15636, 15647, 15824, 15836, 15848, 15860, 15872, 16049, 16061, 16073, 16085, 16097, 16292, 16304, 16316, 16328, 16340, 16517, 16530, 16542, 16554, 16565, 16742, 16754, 16766, 16778, 16790, 16967, 16979, 16991, 17003, 17015, 17192, 17204, 17216, 17228, 17240, 17453, 17465, 17477, 17489, 17501, 17678, 17690, 17702, 17714, 17726, 17738, 17750, 17762, 17774, 17786, 17798, 17810, 17823, 17835, 17847, 17858, 17870, 17882, 17894, 17906, 17918, 17930, 17942, 1904, 1916, 1928, 19337, 19349, 19361, 19373, 19385, 1940, 1952, 19562, 19574, 19586, 19598, 19610, 20416, 20428, 20440, 20452, 20463, 20640, 20653, 20665, 20677, 20688, 2129, 2141, 2153, 2165, 2177, 22831, 22843, 22856, 22868, 22880, 22892, 22904, 22916, 22928, 22940, 22952, 22964, 22976, 22988, 23000, 23017, 23023, 23036, 23048, 23059, 23071, 23083, 23095, 2354, 2366, 2378, 23886, 23898, 2390, 23910, 23922, 23934, 2402, 24111, 24123, 24135, 24147, 24159, 24336, 24348, 24360, 24372, 24384, 24561, 24574, 24586, 24598, 24609, 24786, 24798, 24810, 24822, 24834, 25011, 25023, 25035, 25047, 25059, 25236, 25248, 25260, 25272, 25284, 2579, 2591, 2603, 26088, 26100, 26112, 26124, 26136, 2615, 2627, 26313, 26325, 26337, 26349, 26361, 26538, 26550, 26562, 26574, 26586, 28056, 28068, 28080, 28092, 28104, 28282, 28294, 28306, 28318, 28330, 28342, 28354, 28366, 28378, 28390, 28402, 28414, 28425, 28437, 28449, 28461, 28473, 28485, 28497, 28509, 28521, 28533, 28545, 29214, 29226, 29238, 29250, 29262, 29439, 29451, 29463, 29475, 29487, 31025, 31037, 31049, 31061, 31073, 31250, 31262, 31274, 31286, 31298, 31475, 31487, 31499, 31511, 31523, 31736, 31748, 31761, 31773, 31784, 33218, 33230, 33242, 33254, 33266, 33443, 33455, 33467, 33479, 33491, 33668, 33680, 33692, 33704, 33716, 33911, 33923, 33935, 33947, 33959, 34136, 34149, 34161, 34173, 34184, 3440, 3452, 3464, 3476, 3488, 34990, 35002, 35014, 35026, 35038, 35215, 35227, 35239, 35251, 35263, 36067, 36079, 36091, 36103, 36115, 36292, 36304, 36316, 36328, 36340, 37232, 37244, 37257, 37269, 37280, 38160, 38172, 38184, 38196, 38208, 4288, 4300, 4312, 4324, 4336, 4348, 4361, 4373, 4385, 4397, 4409, 4421, 4433, 4445, 4456, 4468, 4606, 4618, 4630, 4642, 4654, 4831, 4843, 4855, 4867, 4879, 5056, 5068, 5080, 5092, 5104, 5281, 5293, 5305, 5317, 5329, 538, 550, 5506, 5518, 5530, 5542, 5554, 562, 5731, 574, 5743, 5755, 5767, 5779, 586, 5956, 5969, 5982, 5993, 6004, 6181, 6193, 6205, 6217, 6229, 6406, 6418, 6430, 6442, 6454, 6631, 6643, 6655, 6667, 6679, 763, 775, 787, 799, 811, 8182, 8194, 8206, 8218, 8230, 8407, 8419, 8431, 8443, 8455, 8632, 8644, 8656, 8668, 8680, 8857, 8869, 8881, 8893, 8905, 9082, 9094, 9106, 9118, 9130, 9307, 9319, 9331, 9343, 9355, 9532, 9544, 9556, 9568, 9580, 9757, 9769, 9781, 9793, 9805, 988, 9982, 9994, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: /dids/xnt_020027/raw_records-rfzvpzj4mf (Caused by NewConnectionError())         in lines [23810, 23811, 23812, ]

========= Output for: input_files/sr0_bkg_batch_0_0.txt =========
Error msg: ERROR  ConnectionError: (, RemoteDisconnected())      in lines [39285, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: /dids/xnt_031122/distinct_channels-yvrgi4jepp-000000 (Caused by NewConnectionError())    in lines [50839, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: /auth/x509_proxy (Caused by NewConnectionError())        in lines [41058, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: /replicas/list (Caused by NewConnectionError())          in lines [44246, ]
Error msg: ERROR  ConnectionError: HTTPSConnectionPool(host=, port=443): Max retries exceeded with url: /rses/UC_MIDWAY_USERDISK (Caused by NewConnectionError())        in lines [41061, ]
Error msg: ERROR:  Command exited with non-zero exit code (11): /usr/bin/stashcp         in lines [45491, 45504, 45517, 45530, 45543, 46227, 46240, 46253, 46266, 46279, ]

Execution time: 0.036517 s
```
