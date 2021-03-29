.. _hardware_and_firmware_versions:

==============================
Hardware and firmware versions
==============================

Unfortunately, we can't carry out automatic dependency (version) resolution of the hardware or firmware that
``data-gateway`` runs on as the packages are not controlled by ``pip`` (the python dependency manager). We can,
however, manually specify which hardware/firmware versions are compatible with this package. So far, the following
versions of this package have been written to work with the respectively listed versions of firmware/hardware:

0.0.5
-----

* ``v0.1.1`` of `mr_firmware <https://gitlab.com/windenergie-hsr/aerosense/electronics/mr_firmware>`_
* ``v0.1.0`` of `basestation-firmware <https://gitlab.com/windenergie-hsr/aerosense/electronics/basestation-firmware>`_
* ``v1.0.0`` of `mr-pcb <https://gitlab.com/windenergie-hsr/aerosense/electronics/pcbs/mr-pcb>`_


0.0.4
-----

* ``v0.1.0`` of `mr_firmware <https://gitlab.com/windenergie-hsr/aerosense/electronics/mr_firmware>`_
* ``v0.1.0`` of `basestation-firmware <https://gitlab.com/windenergie-hsr/aerosense/electronics/basestation-firmware>`_
* ``v1.0.0`` of `mr-pcb <https://gitlab.com/windenergie-hsr/aerosense/electronics/pcbs/mr-pcb>`_

As this is only the first version of ``data-gateway``, we decided to provide some kind of mapping of hardware/firmware
to software when support for a new version of hardware/firmware is needed to be used, rather than providing this right
now. As new versions of the firmware/hardware are produced, the authors will need to supply us with test fixture data
so we can make sure ``data-gateway`` is compatible.

More information and discussion on this topic can be found
`here <https://gitlab.com/windenergie-hsr/aerosense/digital-twin/data-gateway/-/issues/12>`_.
