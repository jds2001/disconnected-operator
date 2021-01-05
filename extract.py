#!/usr/bin/env python

import os
import sys
import sqlite3
import argparse
import tarfile
import json
import shlex
import subprocess
import tempfile

def extract_layer(layername):
    layer = tarfile.open(name=layername, mode='r:gz')
    for member in layer:
        if member.name.startswith('/'):
            sys.stderr.write('Bad file! Membber has absolute path\n')
            sys.stderr.write('Offending layer: {0}, file: {1}'.format(layername, member.name))
            sys.exit(1)
        #print('{}'.format(member.name))
        if member.isdev():
            print('skipping device file {}'.format(member.name))
            continue
        dest=os.path.join(os.getcwd(), member.name)
        if not member.isdir() and os.path.exists(dest):
            print('deleting {} in favor of new'.format(member.name))
            os.unlink(dest)
        layer.extract(member, set_attrs=False, path=os.getcwd())
    return True

def get_layers(dirname):
    os.chdir(dirname)
    layers=[]
    manifest=json.loads(open('manifest.json').read())
    for layer in manifest['layers']:
        layers.append(layer['digest'][7:])
    return layers

def read_needed_operators(filename):
    '''This takes a file that has pipe-separated values for package and channel
    names, and returns a dictionary with keys being the package names, and the
    values being a list of channels needed for that operator. For example,
    you can say you need 2.4 and stable of kubevirt-hyperconverged'''

    needlines=open(filename, 'r').readlines()
    needed={}
    for line in needlines:
        if line.startswith('#'):
            continue
        package, channel = line.split('|')
        if package in needed.keys():
            needed[package].append(channel.strip())
        else:
            needed[package]=[channel.strip()]
    return needed

def find_related_images(database, package, channel):
    related_images=[]
    conn=sqlite3.connect(database)
    params=(package, channel)
    c=conn.execute('SELECT * FROM channel WHERE package_name=? AND name=?', params)
    for item in c:
        channel, package, bundle = item
        print(bundle)
        images=conn.execute('SELECT image FROM related_image WHERE operatorbundle_name=?', (bundle,))
        for image in images:
            related_images.append(image[0])
    return bundle, related_images

def get_image(image, tag, skopeo_binary, verbose=False):
    temp_directory=tempfile.mkdtemp(dir=os.getcwd())
    print('temporary dir is {}'.format(temp_directory))
    os.chdir(temp_directory)
    raw_command='{0} copy docker://{1}:{3} dir://{2}'.format(skopeo_binary, image, temp_directory, tag)
    command=shlex.split(raw_command)
    if verbose:
        print(command)
    comm=subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if verbose:
        print(comm.stdout, comm.stderr)
    return temp_directory

def list_channels(database):
    conn=sqlite3.connect(database)
    c=conn.execute('SELECT package_name, name FROM channel')
    for item in c:
        print('{0}|{1}'.format(item[0], item[1]))

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-f', '--filename', default='operators.txt', help='Operators to fetch, see docs for format')
    parser.add_argument('-i', '--image', default='registry.redhat.io/redhat/redhat-operator-index', help='Image of operator index to fetch')
    parser.add_argument('-t', '--tag', default='v4.6', help='Image tag to fetch')
    parser.add_argument('-d', '--database', default='database/index.db',
            help='Path of sqlite3 database')
    parser.add_argument('-l', '--list-channels', action='store_true', help='List available channels in image')
    parser.add_argument('--skopeo', help='Path to skopeo binary if non-default', default='/usr/bin/skopeo')
    parser.add_argument('--no-image', action='store_true', help='Dont get the image, must specify sqlite db')
    args=parser.parse_args()
    needed=read_needed_operators(args.filename)
    print(needed)
    if not args.no_image:
        tmpdir=get_image(args.image, args.tag, args.skopeo, verbose=args.verbose)
        layers=get_layers(tmpdir)
        os.chdir(tmpdir)
        for layer in layers:
            extract_layer(layer)
    else:
        tmpdir=os.getcwd()
    os.chdir(tmpdir)
    print(tmpdir)
    if args.list_channels:
        list_channels(args.database)
        sys.exit(0)
    images={}
    for operator in needed.keys():
        for channel in needed[operator]:
            needed_images=find_related_images(args.database, operator, channel)
            images[needed_images[0]]=needed_images[1]
    print(images)
