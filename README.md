# CoreOS Disk Image Rehydrator

Part of implementing https://github.com/openshift/enhancements/pull/201

The basic idea here is: Ship a container image that contains:

 - The OS update data (an ostree commit)
 - This executable
 - Extra "recipe" sufficient to generate ideally all the disk images, from
   the `-qemu.qcow2` and the `-aws.vmdk` disk images, the `.iso` etc - but without
   duplicating all the disk image data entirely as that would add up *fast*.

## First approach: reuse "coreos-installer osmet" to synthesize the qemu.qcow2 from the ostree data

https://github.com/coreos/coreos-installer/blob/master/docs/osmet.md

This would be ideal if we can directly generate the `-qemu.qcow2` from
the ostree data.  There are some differences here though; in this
case we have an archive mode ostree repo (currently).

But, failing that it won't be the end of the world if we punt on
this and ship a "base image" whether that's the `-qemu.qcow2` or the `.iso`
or whatever separately from the ostree update, and use that as the source.

## Compression

We need to get this out of the way: compression gets very hard to reproduce.
See https://manpages.debian.org/unstable/pristine-tar/pristine-tar.1.en.html

Our initial goal will be to generate *uncompressed* images, and verify
the uncompressed SHA-256 matches.  That's all we need to be sure we've
generated the same thing - we don't need to replicate the compression exactly.

Hence, while we ship e.g. `-qemu.qcow2.xz`, we will primarily operate on
`-qemu.qcow2`.

## Differences: ignition.platform.id

The `-openstack.qcow2` and the `-qemu.qcow2` only differ in the
`ignition.platform.id` in the boot partition that is written
by https://github.com/coreos/coreos-assembler/blob/master/src/gf-platformid

Let's start there - can we do a binary diff between the two images
and generate a small "recipe" that reliably resythesizes the `-openstack.qcow2`
from the `-qemu.qcow2`?

Hopefully the differences are small.

## VMDK

AWS and vSphere are "VMDK" images which have internal compression.  cosa
today uses an invocation like this:

```
$ qemu-img convert -O vmdk -f qcow2 -o adapter_type=lsilogic,subformat=streamOptimized,compat6 fcos-qemu.qcow2 fcos-aws.vmdk
```

And this `streamOptimized` bit seems to turn on internal compression.
Further, there's a bit of random data generated during this process:
https://github.com/qemu/qemu/blob/266469947161aa10b1d36843580d369d5aa38589/block/vmdk.c#L2519

So...a strategy here may be to:

- Patch the platform ID as above
- run the `qemu-img convert` invocation above
- Have a postprocessing step that patches the `CID` to the originally known value
  stored in the residue (or we could just make it zeroed initially in cosa builds?)

## ISO

Going to be very hard because I doubt squashfs is reproducible at all.  We may
need to just punt on this and ship the ISO as is.  Which, in turn may inform
whether it should actually be the source image?


### What is "rehydration"?

A tip of the hat to https://en.wikipedia.org/wiki/The_Three-Body_Problem_(novel) where an alien species can "dehydrate" during times of crisis and "rehydrate" when it's over.
Here crisis is analogous to compression, rehydration to decompression.
