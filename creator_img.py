import humanfriendly
import os
import tempfile
import uuid
from sh import fallocate, parted, losetup, mkfs, mount, umount, kolibri


class KolibriContentSelection(object):
    def __init__(
        self,
        channel_id,
        include_node_ids=None,
        exclude_node_ids=None,
        method="network",
        path="https://studio.learningequality.org/",
    ):
        self.channel_id = channel_id
        self.include_node_ids = include_node_ids
        self.exclude_node_ids = exclude_node_ids
        self.method = method
        self.path = path

    def run_import(self):
        print("Importing content from channel {}...".format(self.channel_id))

        try:

            # download the channel database
            kolibri("manage", "importchannel", "network", self.channel_id, _in="y\n")

            # download the requested contnt from the channel
            args = ["manage", "importcontent", "network", self.channel_id]
            if self.include_node_ids:
                args += ["--node_ids", ",".join(self.include_node_ids)]
            if self.exclude_node_ids:
                args += ["--exclude_node_ids", ",".join(self.exclude_node_ids)]
            kolibri(*args, _in="y\n")

        except Exception as e:
            error = e.stderr.decode()
            print("ERROR DOWNLOADING CHANNEL {}...".format(self.channel_id))
            print(error)


class KolibriDiskImageCreator(object):
    def __init__(self, content_list, disk_size="100MB", image_path="/tmp"):
        self.content_list = content_list
        self.disk_size_bytes = humanfriendly.parse_size(disk_size)
        if not image_path.endswith(".img"):
            image_path = os.path.join(image_path, uuid.uuid4().hex + ".img")
        self.image_path = image_path
        self.mount_path = tempfile.mkdtemp()
        os.environ["KOLIBRI_HOME"] = os.path.join(self.mount_path, "KOLIBRI_DATA")
        os.environ["KOLIBRI_RUN_MODE"] = "kolibridiskimagecreator"
        self.mounted = False
        self.loop = None

    def create(self):
        self.init_image()
        self.mount()
        for selection in self.content_list:
            selection.run_import()
        self.unmount()
        print("Finished creating disk image at: {}".format(self.image_path))
        return self.image_path

    def init_image(self):
        fallocate("-l", str(self.disk_size_bytes), self.image_path)
        output = parted(
            self.image_path,
            "mklabel",
            "msdos",
            "mkpart",
            "primary",
            "fat32",
            "2048s",
            "100%",
            "print",
        )
        self.offset = (
            [line for line in output.split("\n") if line.strip().startswith("1 ")][0]
            .split()[1]
            .replace("B", "")
        )
        self._ensure_loopback_device()
        mkfs("-t", "vfat", "-F", "32", self.loop)

    def _ensure_loopback_device(self):
        if not self.loop:
            self.loop = losetup(
                "--partscan", "--show", "--find", self.image_path
            ).strip()

    def mount(self):
        assert not self.mounted, "Image has already been mounted"
        assert self.offset, "Image must first be initialized with init_image"
        self._ensure_loopback_device()
        mount(self.loop, self.mount_path)
        self.mounted = True

    def unmount(self):
        assert self.mounted, "Image is not mounted"
        umount(self.mount_path)
        losetup("-d", self.loop)
        self.loop = None
        self.mounted = False


# c = KolibriDiskImageCreator(
#     content_list=[
#         KolibriContentSelection(channel_id="17e25cd51c1842dd87755dcd7cd515a4")
#     ]
# )
# c.create()
