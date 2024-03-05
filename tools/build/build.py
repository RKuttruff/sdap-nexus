# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import shutil
import subprocess
import tempfile
import hashlib
from tenacity import retry, stop_after_attempt, wait_fixed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

DOCKER = shutil.which('docker')

NEXUS_TARBALL = 'apache-sdap-nexus-{}-incubating-src.tar.gz'
INGESTER_TARBALL = 'apache-sdap-ingester-{}-incubating-src.tar.gz'

NEXUS_DIR = 'apache-sdap-nexus-{}-incubating-src'
INGESTER_DIR = 'apache-sdap-ingester-{}-incubating-src'

HAS_GRADUATED = False

if DOCKER is None:
    raise OSError('docker command could not be found in PATH')


def build_cmd(tag, context, dockerfile='', cache=True):
    command = [DOCKER, 'build', context]

    if dockerfile != '':
        command.extend(['-f', os.path.join(context, dockerfile)])

    command.extend(['-t', tag])

    if not cache:
        command.append('--no-cache')

    return command


@retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
def run(cmd, suppress_output=False, err_on_fail=True):
    stdout = subprocess.DEVNULL if suppress_output else None

    p = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT)

    p.wait()

    if err_on_fail and p.returncode != 0:
        raise OSError(f'Subprocess returned nonzero: {p.returncode}')


def yes_no(prompt):
    do_continue = input(prompt).lower()

    while do_continue not in ['', 'y', 'n']:
        do_continue = input(prompt).lower()

    return do_continue in ['', 'y']


def choice_prompt(prompt: str, choices: list, default: str = None) -> str:
    assert len(choices) > 0

    if len(choices) == 1:
        return choices[0]

    valid_choices = [str(i) for i in range(len(choices))]

    if default is not None:
        assert default in choices
        valid_choices.append('')

    print(prompt)

    choice = None

    while choice not in valid_choices:
        for i, c in enumerate(choices):
            print('[{:2d}] {}-> {}'.format(i, ''.ljust(10, '-'), c))

        print()

        if default is None:
            choice = input('Selection: ')
        else:
            choice = input(f'Selection [{choices.index(default)}]: ')

    if default is not None and choice == '':
        return default
    else:
        return choices[int(choice)]


def get_input(prompt):
    while True:
        response = input(prompt)

        if yes_no(f'Confirm: "{response}" [Y]/N '):
            return response


def pull_source(dst_dir: tempfile.TemporaryDirectory, args: argparse.Namespace):
    ASF = 'ASF subversion (dist.apache.org)'
    GIT = 'GitHub (Not implemented yet)'
    LFS = 'Local Filesystem (Not implemented yet)'

    source_location = choice_prompt(
        'Where is the source you\'re building from stored?',
        [ASF, GIT, LFS],
        ASF
    )

    if source_location == ASF:
        area = 'dev' if yes_no('Is this a release candidate? (No = official release) [Y]/N: ') else 'release'
        url = f'https://dist.apache.org/repos/dist/{area}/'

        if HAS_GRADUATED:
            raise NotImplementedError()
        else:
            url = url + 'incubator/sdap/'

        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        version = choice_prompt(
            'Choose a release/release candidate to build',
            [node.text.rstrip('/') for node in soup.find_all('a') if node.get('href').rstrip('/') not in ['KEYS', '..']],
        )

        url = url + version + '/'

        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        def remove_suffixes(s: str, suffixes):
            for suffix in suffixes:
                s = s.removesuffix(suffix)

            return s

        build_artifacts = list(set([remove_suffixes(node.text, ['.sha512', '.asc']) for node in soup.find_all('a') if node.get('href').rstrip('/') not in ['KEYS', '..']]))

        nexus_tarball = None
        ingester_tarball = None

        for artifact in build_artifacts:
            if '-nexus-' in artifact and not args.skip_nexus:
                nexus_tarball = os.path.join(dst_dir.name, artifact)
            elif '-ingester-' in artifact and not args.skip_ingester:
                ingester_tarball = os.path.join(dst_dir.name, artifact)

            for ext in ['', '.sha512', '.asc']:
                filename = artifact + ext
                dst = os.path.join(dst_dir.name, filename)

                print(f'Downloading {url + artifact + ext}')

                response = requests.get(url + artifact + ext)
                response.raise_for_status()

                with open(dst, 'wb') as fp:
                    fp.write(response.content)
                    fp.flush()

            print(f'Verifying checksum for {artifact}')

            m = hashlib.sha512()
            m.update(open(os.path.join(dst_dir.name, artifact), 'rb').read())

            if m.hexdigest() != open(os.path.join(dst_dir.name, artifact) + '.sha512', 'r').read().split(' ')[0]:
                raise ValueError('Bad checksum!')

            print(f'Verifying signature for {artifact}')

            try:
                run(
                    ['gpg', '--verify', os.path.join(dst_dir.name, artifact) + '.asc', os.path.join(dst_dir.name, artifact)],
                    True
                )
            except:
                raise ValueError('Bad signature!')

        print('Extracting release source files...')

        if not args.skip_nexus:
            run(
                ['tar', 'xvf', nexus_tarball, '-C', dst_dir.name],
                suppress_output=True
            )
            shutil.move(
                os.path.join(dst_dir.name, 'Apache-SDAP', nexus_tarball.split('/')[-1].removesuffix('.tar.gz')),
                os.path.join(dst_dir.name, 'nexus')
            )

        if not args.skip_ingester:
            run(
                ['tar', 'xvf', ingester_tarball, '-C', dst_dir.name],
                suppress_output=True
            )
            shutil.move(
                os.path.join(dst_dir.name, 'Apache-SDAP', ingester_tarball.split('/')[-1].removesuffix('.tar.gz')),
                os.path.join(dst_dir.name, 'ingester')
            )
    elif source_location == GIT:
        raise NotImplementedError()
    else:
        raise NotImplementedError()


def main():
    parser = argparse.ArgumentParser(
        epilog="With the exception of the --skip-nexus, --skip-ingester, and --tag-suffix options, the user will be "
               "prompted to set options at runtime."
    )

    parser.add_argument(
        '-t', '--tag',
        dest='tag',
        help='Tag for built docker images',
    )

    parser.add_argument(
        '--docker-registry',
        dest='registry',
        help='Docker registry to tag images with. Important if you want to push the images.'
    )

    cache = parser.add_mutually_exclusive_group(required=False)

    cache.add_argument(
        '--no-cache',
        dest='cache',
        action='store_false',
        help='Don\'t use build cache'
    )

    cache.add_argument(
        '--cache',
        dest='cache',
        action='store_true',
        help='Use build cache'
    )

    push = parser.add_mutually_exclusive_group(required=False)

    push.add_argument(
        '--push',
        dest='push',
        action='store_true',
        help='Push images after building'
    )

    push.add_argument(
        '--no-push',
        dest='push',
        action='store_false',
        help='Don\'t push images after building'
    )

    parser.add_argument(
        '--skip-nexus',
        dest='skip_nexus',
        action='store_true',
        help='Don\'t build Nexus webapp, Solr cloud & Solr cloud init images'
    )

    parser.add_argument(
        '--skip-ingester',
        dest='skip_ingester',
        action='store_true',
        help='Don\'t build Collection Manager & Granule Ingester images'
    )

    parser.set_defaults(cache=None, push=None)

    args = parser.parse_args()

    tag, registry, cache, push = args.tag, args.registry, args.cache, args.push

    # TODO: Support pulling from any ASF release/stage location, OR git repo OR local FS. And make it more interactive

    if tag is None:
        tag = get_input('Enter the tag to use for built images: ')

    if registry is None:
        registry = get_input('Enter Docker image registry: ')

    if cache is None:
        cache = yes_no('Use Docker build cache? [Y]/N: ')

    if push is None:
        push = yes_no('Push built images? [Y]/N: ')

    print('Downloading release tarballs...')

    extract_dir = tempfile.TemporaryDirectory()

    pull_source(extract_dir, args)

    os.environ['DOCKER_DEFAULT_PLATFORM'] = 'linux/amd64'

    built_images = []

    if not args.skip_ingester:
        print('Building ingester images...')

        cm_tag = f'{registry}/sdap-collection-manager:{tag}'

        run(build_cmd(
            cm_tag,
            os.path.join(extract_dir.name, 'ingester'),
            dockerfile='collection_manager/docker/Dockerfile',
            cache=cache
        ))

        built_images.append(cm_tag)

        gi_tag = f'{registry}/sdap-granule-ingester:{tag}'

        run(build_cmd(
            gi_tag,
            os.path.join(extract_dir.name, 'ingester'),
            dockerfile='granule_ingester/docker/Dockerfile',
            cache=cache
        ))

        built_images.append(gi_tag)

    if not args.skip_nexus:
        solr_tag = f'{registry}/sdap-solr-cloud:{tag}'

        run(build_cmd(
            solr_tag,
            os.path.join(extract_dir.name, 'nexus/docker/solr'),
            cache=cache
        ))

        built_images.append(solr_tag)

        solr_init_tag = f'{registry}/sdap-solr-cloud-init:{tag}'

        run(build_cmd(
            solr_init_tag,
            os.path.join(extract_dir.name, 'nexus/docker/solr'),
            dockerfile='cloud-init/Dockerfile',
            cache=cache
        ))

        built_images.append(solr_init_tag)

        webapp_tag = f'{registry}/sdap-nexus-webapp:{tag}'

        run(build_cmd(
            webapp_tag,
            os.path.join(extract_dir.name, 'nexus'),
            dockerfile='docker/nexus-webapp/Dockerfile',
            cache=cache
        ))

        built_images.append(webapp_tag)

    print('Image builds completed')

    if push:
        for image in built_images:
            run(
                [DOCKER, 'push', image]
            )

    print('done')


if __name__ == '__main__':
    main()
