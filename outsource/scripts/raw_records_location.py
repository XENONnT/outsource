import sys
from utilix import DB
from straxen import DAQReader


def main():
    run_id = sys.argv[1]

    db = DB()
    run_data = db.get_data(int(run_id))
    n = max(len(data_type) for data_type in DAQReader.provides)

    for data_type in DAQReader.provides:
        locations = []
        for data in run_data:
            if (
                data["type"] == data_type
                and data["host"] == "rucio-catalogue"
                and data["status"] == "transferred"
                and "TAPE" not in data["location"]
            ):
                locations.append(data["location"])
        print(f"{data_type.ljust(n + 1)}: {' '.join(locations)}")


if __name__ == "__main__":
    main()
