This directory contains a Wireshark dissector for the RaptorCast protocol.

This dissector supports version 0 of the RaptorCast protocol, which is
the current and latest version, and supports the following features:

- Decoding and displaying of all protocol header fields, with foldable
  signature and Merkle proof sections.
- Naive validity checking of protocol fields for which it is feasible.
- Simple packet delay estimation for received RaptorCast packets, based
  on the assumption that the time-of-day clock of the packet sender is
  approximately in sync with our own.
- Merkle root reconstruction for all valid Merkle tree depths (1-9).
- Signature validation and author public key recovery with caching.

For a list of open to-do items, see the note at the top of the
`raptorcast.c` file.


CLI usage examples
------------------

To inspect a pcap file containing RaptorCast traffic, run:

```
$ tshark -r raptorcast.pcap -V | less
```

To extract specific RaptorCast header fields and produce output in
Tab-separated value format, for example for piping into traffic
analysis tools, try running something like this:

```
tshark -r raptorcast.pcap -V -T fields -E separator=/t \
        -e frame.time_relative \
        -e ip.src_host \
        -e ip.dst_host \
        -e udp.srcport \
        -e udp.dstport \
        -e raptorcast.author \
        -e raptorcast.broadcast \
        -e raptorcast.unix_ts_ms \
        -e raptorcast.delay_ms \
        -e raptorcast.app_message_hash \
        -e raptorcast.app_message_len \
        -e raptorcast.merkle_root \
        -e raptorcast.first_hop_recipient \
        -e raptorcast.merkle_leaf_index \
        -e raptorcast.encoding_symbol_id \
        -e raptorcast.encoded_symbol_len
```


Installing the dissector
------------------------

First, make sure you have wireshark installed:

```
# dnf install wireshark    # Fedora 41
# apt install wireshark    # Ubuntu 25.04
```

If you care only about the command-line interface to Wireshark, run
this instead:

```
# dnf install wireshark-cli   # Fedora 41
# apt install tshark          # Ubuntu 25.04
```

Then build the dissector (see below), and copy the `raptorcast.so` file
to the wireshark plug-in directory in your home directory, as follows:

```
# Fedora 41
mkdir -p ~/.local/lib/wireshark/plugins/4.4/epan
cp raptorcast.so ~/.local/lib/wireshark/plugins/4.4/epan/

# Ubuntu 25.04
mkdir -p ~/.local/lib/wireshark/plugins/4.2/epan
cp raptorcast.so ~/.local/lib/wireshark/plugins/4.2/epan/
```


Building the dissector for Fedora 41
------------------------------------

To build the dissector for Fedora 41 in a Docker container, run the
following commands:

```
docker image pull fedora:41
docker run --interactive --mount type=bind,src=/home/$USER,dst=/home/$USER --tty --env SRCDIR=`pwd` fedora:41 /bin/bash
dnf -y upgrade
dnf -y install @development-tools blake3-devel cmake libsecp256k1-devel wireshark-devel
cd $SRCDIR
rm -rf build
mkdir build
cd build
cmake ..
make
```

Building the dissector for Ubuntu 25.04
---------------------------------------

Ubuntu 25.04 doesn't ship with `blake3`, so the Docker build is a
little bit more involved:

```
docker image pull ubuntu:25.04
docker run --interactive --mount type=bind,src=/home/$USER,dst=/home/$USER --tty --env HOME=$HOME --env SRCDIR=`pwd` ubuntu:25.04 /bin/bash
apt update
ln -s /usr/share/zoneinfo/posix/UTC /etc/localtime
apt -y dist-upgrade
apt -y install build-essential cmake git libsecp256k1-dev wireshark-dev

cd /tmp
git clone https://github.com/BLAKE3-team/BLAKE3/
cd BLAKE3/c
mkdir build
cd build
cmake -DBUILD_SHARED_LIBS=1 ..
make
make install
ln -s ../local/lib/libblake3.so /usr/lib64/libblake3.so

ln -s ../lib/x86_64-linux-gnu/libsecp256k1.so /usr/lib64/libsecp256k1.so

cd $SRCDIR
rm -rf build
mkdir build
cd build
cmake ..
make
```

To use the dissector outside of the Docker container, you will probably
have to copy the `libblake3.so*` libraries to a directory from which
Wireshark can dynamically link them:

```
mkdir ~/wireshark-libs
cd ~/wireshark-libs
cp -a /usr/local/lib/libblake3.so* .
```

And you'll have to use a wrapper script to allow for this dynamic linking
to happen:

```
mkdir -p ~/bin
cd ~/bin
cat > tshark <<EOF
#!/bin/sh

export LD_LIBRARY_PATH=$HOME/wireshark-libs

exec /usr/bin/tshark "\$@"
EOF
chmod a+x tshark
```
