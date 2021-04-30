# CoreOS Disk Image Rehydrator

Part of implementing https://github.com/openshift/enhancements/pull/201

The basic idea here is: Ship a container image that contains:

 - This executable
 - A "base image" used to generate others (may be qemu or ISO, see below)
 - Extra "recipe" sufficient to generate all the other disk images on demand, from
   the `-qemu.qcow2` and the `-aws.vmdk` disk images, the `.iso` etc - but without
   duplicating all the disk image data entirely as that would add up *fast*.

# Requirement: Bit-for-bit uncompressed SHA-256 match

Our CI tests the disk images.  In order to ensure that we're
re-generating what we tested, it's very imporant that the
uncompressed SHA-256 match.

## Image differences

The `-openstack.qcow2` and the `-qemu.qcow2` only differ in the
`ignition.platform.id` in the boot partition that is written
by https://github.com/coreos/coreos-assembler/blob/master/src/gf-platformid

However, the way we replace this also causes e.g. filesystem metadata (extents, timestamps)
to change.

Plus, on s390x we need to rerun `zipl` which changes another bit of
data.

## Compression

We need to get this out of the way: compression gets very hard to reproduce.
See https://manpages.debian.org/unstable/pristine-tar/pristine-tar.1.en.html

Our initial goal will be to generate *uncompressed* images, and verify
the uncompressed SHA-256 matches.  That's all we need to be sure we've
generated the same thing - we don't need to replicate the compression exactly.
We can trust our compression tools.

Hence, while we ship e.g. `-qemu.qcow2.xz` (or `.gz` for RHCOS currently),
we will primarily generate e.g. `-qemu.qcow2` and for callers that want
it compressed, we may pass it to `gzip -1` or so.

## First approach: Add the -qemu.qcow2 image and use rsync to regenerate most images

A lowest common denominator to de-duplicate these is rsync-style rolling
checksums.  This approach is also used by ostree "baseline" deltas, although
it can also use bsdiff.

## Other approach: Reuse oscontainer content

We need to have a separate container image from `machine-os-content` in the RHCOS case,
because today any change to `machine-os-content` will cause nodes to update on *all platforms*.  But we
don't want to force every machine to reboot just because we needed to respin the
vsphere OVA!

However, what may work well is to have our image do
`FROM quay.io/openshift/machine-os-content@sha256:...`

Today the rhcos oscontainer is an "archive" mode repo (each file individually compressed).
In the future with ostree-ext containers we'll have an uncompressed repo, which
will be easier to use as a deduplication source.

Either way, what may work is to e.g. generate a delta from "oscontainer stream" (tarball e.g.)
to the metal/qcow2 image, and then deltas from that to other images.

## Related: osmet?

https://github.com/coreos/coreos-installer/blob/master/docs/osmet.md

We could be much smarter about our deltas with an osmet-style approach.  We even ship the ISO
which has osmet for the metal images inside it.

So a simple approach could be to ship the ISO as the basis, and launch it in qemu to
have it generate the metal image.  Then we use the metal image as an rsync-style rolling
basis for everything else.

See below for more on the ISO.

## VMDK

AWS and vSphere are "VMDK" images which have internal compression.  cosa
today uses an invocation like this:

```
$ qemu-img convert -O vmdk -f qcow2 -o adapter_type=lsilogic,subformat=streamOptimized,compat6 fcos-qemu.qcow2 fcos-aws.vmdk
```

And this `streamOptimized` bit seems to turn on internal compression.
Further, there's a bit of random data generated during this process:
https://github.com/qemu/qemu/blob/266469947161aa10b1d36843580d369d5aa38589/block/vmdk.c#L2519
(Which will be handled by the rsync-style delta)

For now, let's hardcode the qemu options for these two here again.  But longer
term perhaps we fork off `qemu-img info` to try to gather this, or change coreos-assembler
to include the `qemu-img` options used to generate the image.

## ISO

The structure of the ISO is mostly a wrapper for `images/pxeboot/rootfs.img` which 
is a CPIO blob which contains a `squashfs` plus the osmet glue.

Reproducing squashfs bit may require using a fork: 

- https://github.com/NixOS/nixpkgs/issues/40144
- https://reproducible-builds.org/docs/system-images/

### What is "rehydration"?

A tip of the hat to https://en.wikipedia.org/wiki/The_Three-Body_Problem_(novel) where an alien species can "dehydrate" during times of crisis and "rehydrate" when it's over.
Here crisis is analogous to compression, rehydration to decompression.
