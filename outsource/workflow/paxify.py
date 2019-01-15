"""Replacement for cax-process"""
import argparse
import json
from pax import core, configuration

# TODO do DB API call here instead of using json
# TODO allow for passing custom config (maybe via json?)

def process(inputfile, output, json_path):
    with open(json_path, "r") as f:
        doc = json.load(f)

    detector = doc['detector']
    name = doc['name']

    if detector == 'muon_veto':
        pax_config = 'XENON1T_MV'
        decoder = 'BSON.DecodeZBSON'

    elif detector == 'tpc':
        decoder = 'Pickle.DecodeZPickle'

        if doc['reader']['self_trigger']:
            pax_config = 'XENON1T'
        else:
            pax_config = 'XENON1T_LED'
    else:
        raise ValueError('Detector must be tpc or muon_veto')

    config_dict = {'pax': {'input_name': inputfile,
                           'output_name': output,
                           'look_for_config_in_runs_db': False,
                           'decoder_plugin': decoder,
                           'stop_after': 10 # temporary
                           }}
    if detector == 'tpc':
        mongo_config = doc['processor']
        config_dict = configuration.combine_configs(mongo_config, config_dict)

    # Add run number and run name to the config_dict
    config_dict.setdefault('DEFAULT', {})
    config_dict['DEFAULT']['run_number'] = doc['number']
    config_dict['DEFAULT']['run_name'] = doc['name']

    pax_kwargs = dict(config_names=pax_config,
                      config_dict=config_dict)

    core.Processor(**pax_kwargs).run()


def main():
    parser = argparse.ArgumentParser(description='Process with pax given a json file')
    parser.add_argument("--input", type=str, help='Input file')
    parser.add_argument("--output", type=str, help='Directory to save output')
    parser.add_argument("--json_path", type=str, help='path to json file')
    args = parser.parse_args()
    process(args.input, args.output, args.json_path)


if __name__ == "__main__":
    main()
