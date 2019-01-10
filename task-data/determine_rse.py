from __future__ import print_function
import subprocess
import sys
import re


EURO_SITES = ["CCIN2P3_USERDISK", 
              "NIKHEF_USERDISK",
              "WEIZMANN_USERDISK", 
              "CNAF_USERDISK", 
              "SURFSARA_USERDISK"]

US_SITES = ["UC_OSG_USERDISK"]

ALL_SITES = US_SITES + EURO_SITES

def get_rses(rucio_name):
    # checks if run with rucio_name is on stash
    out = subprocess.Popen(["rucio", "list-rules", rucio_name], stdout=subprocess.PIPE).stdout.read()
    out = out.decode("utf-8").split("\n")
    rses = []
    for site in ALL_SITES:
        for line in out:
            line = re.sub(' +', ' ', line).split(" ")
            if len(line) > 4 and line[4] == site and line[3][:2] == "OK":
                rses.append(site)
    return rses


def determine_rse(rse_list, glidein_country):
    if glidein_country == "US":
        in_US = False
        for site in US_SITES:
            if site in rse_list:
                return site

        if not in_US:
            print("This run is not in the US so can't be processed here. Exit 255")
            sys.exit(255)

    elif glidein_country == "FR":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "NL":
        for site in reversed(EURO_SITES):
            if site in rse_list:
                return site

    elif glidein_country == "IL":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    elif glidein_country == "IT":
        for site in EURO_SITES:
            if site in rse_list:
                return site

    if US_SITES[0] in rses:
        return US_SITES[0]
    else:
        raise AttributeError("cannot download data")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise ValueError("Requires 2 arguments: (1) rucio rse and (2) glidein country")
    rucio_name = sys.argv[1].split(":")[0] + ":raw"
    rses = get_rses(rucio_name)
    print(determine_rse(rses, sys.argv[2]))
