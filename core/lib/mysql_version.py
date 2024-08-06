#!/usr/bin/env python3

# pyre-ignore-all-errors
"""
Copyright (c) 2017-present, Facebook, Inc.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
"""

from typing_extensions import Self


class MySQLVersion:
    def __init__(self, version_str):
        """
        @param version_str: Value of @@version_comment (not @@version).
        Examples:
        8.0.32-202407011440.prod
        8.0.XX-YYYYMMDDHHmm.dev.alexbud
        """

        segments = version_str.split("-")
        version_segments = segments[0].split(".")
        self._major: int = int(version_segments[0])
        self._minor: int = int(version_segments[1])
        self._release: int = int(version_segments[2])

        build_segments = segments[1].split(".")
        self._build: str = build_segments[0]
        self._prod: bool = build_segments[1] == "prod"

    @property
    def major(self) -> int:
        """
        Major version is the first segment of a version string.
        E.g. 8 in 8.0.32
        """
        return self._major

    @property
    def minor(self) -> int:
        """
        Minor version is the seconds segment of a version string.
        E.g. 0 in 8.0.32
        """
        return self._minor

    @property
    def release(self) -> int:
        """
        Release is the third segment of a version string
        E.g. 32 in 8.0.32
        """
        return self._release

    @property
    def build(self) -> str:
        """
        The build number as a str. E.g. 202407011440 in 8.0.32-202407011440.prod
        """
        return self._build

    @property
    def is_mysql8(self) -> bool:
        """
        Return True if major version is 8.
        """
        return self.major == 8

    @property
    def is_prod(self) -> bool:
        """
        Return True if build is a prod build (not a dev build)
        """
        return self._prod

    def __gt__(self, other: Self):
        if self.major > other.major:
            return True
        elif self.major == other.major:
            if self.minor > other.minor:
                return True
            elif self.minor == other.minor:
                if self.release > other.release:
                    return True
                elif self.release == other.release:
                    if self.build > other.build:
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False

    def __eq__(self, other: Self):
        return (
            self.major == other.major
            and self.minor == other.minor
            and self.release == other.release
            and self.build == other.build
        )

    def __lt__(self, other: Self):
        return not self > other and not self == other

    def __ge__(self, other: Self):
        return not self < other

    def __le__(self, other: Self):
        return not self > other
