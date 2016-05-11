# Copyright 2016 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Generator for ddl
-t type of output to generate - either 'pre_transform_spec' or 'transform_spec'
-o output path
-i path to template file
"""

import getopt
import json
import os.path
import sys


class Generator(object):

    key_name = None

    def generate(self, template_path, source_json_path, output_path):
        print("Generating content at %s with template at %s, using key %s" % (
            output_path, template_path, self.key_name))
        data = []
        with open(source_json_path) as f:
            for line in f:
                json_line = json.loads(line)
                data_line = '(\'%s\',\n\'%s\')' % (
                    json_line[self.key_name], json.dumps(json_line))
                data.append(str(data_line))
        print(data)
        with open(template_path) as f:
            template = f.read()
        with open(output_path, 'w') as write_file:
            write_file.write(template)
            for record in data:
                write_file.write(record)
                write_file.write(',\n')
            write_file.seek(-2, 1)
            write_file.truncate()
            write_file.write(';')


class TransformSpecsGenerator(Generator):

    key_name = 'metric_id'


class PreTransformSpecsGenerator(Generator):

    key_name = 'event_type'


def main():
    # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ht:o:i:s:")
        print('Opts = %s' % opts)
        print('Args = %s' % args)
    except getopt.error as msg:
        print(msg)
        print("for help use --help")
        sys.exit(2)
    script_type = None
    template_path = None
    source_json_path = None
    output_path = None
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        elif o == "-t":
            script_type = a
            if a not in ('pre_transform_spec', 'transform_spec'):
                print('Incorrect output type specified: \'%s\'.\n %s' % (
                    a, __doc__))
                sys.exit(1)
        elif o == "-i":
            template_path = a
            if not os.path.isfile(a):
                print('Cannot find template file at %s' % a)
                sys.exit(1)
        elif o == "-o":
            output_path = a
        elif o == "-s":
            source_json_path = a

    print("Called with type = %s, template_path = %s, source_json_path %s"
          " and output_path = %s" % (
              script_type, template_path, source_json_path, output_path))
    generator = None
    if script_type == 'pre_transform_spec':
        generator = PreTransformSpecsGenerator()
    elif script_type == 'transform_spec':
        generator = TransformSpecsGenerator()
    generator.generate(template_path, source_json_path, output_path)

if __name__ == '__main__':
    main()
