"""
Semantic Version Model for ma114tsdb

Constitutional principle: V. Versioned Evolution
All contracts, data formats, and interfaces are versioned using semantic versioning.
"""

from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True)
class Version:
    """
    Semantic version for data format evolution.

    Supports N-1 backward compatibility per FR-046.

    Attributes:
        major: Breaking changes increment (>= 0)
        minor: Backward-compatible additions (>= 0)
    """

    major: int
    minor: int

    def __post_init__(self) -> None:
        """Validate version numbers are non-negative."""
        if self.major < 0:
            raise ValueError(f"major version must be >= 0, got {self.major}")
        if self.minor < 0:
            raise ValueError(f"minor version must be >= 0, got {self.minor}")

    def is_compatible(self, other: Self) -> bool:
        """
        Check N-1 backward compatibility.

        Args:
            other: Version to check compatibility against

        Returns:
            True if other version is compatible with this version

        Compatibility rules (FR-046):
        - Same major version: Always compatible
        - N-1 major version: Compatible (with deprecation warning)
        - Older than N-1 or future major: Incompatible

        Examples:
            >>> v2 = Version(2, 0)
            >>> v2.is_compatible(Version(2, 5))  # Same major
            True
            >>> v2.is_compatible(Version(1, 9))  # N-1
            True
            >>> v2.is_compatible(Version(0, 1))  # Older than N-1
            False
            >>> v2.is_compatible(Version(3, 0))  # Future major
            False
        """
        # Same major version always compatible
        if self.major == other.major:
            return True

        # N-1 backward compatibility
        if self.major == other.major + 1:
            return True

        return False

    def __str__(self) -> str:
        """String representation: major.minor"""
        return f"{self.major}.{self.minor}"

    def __lt__(self, other: Self) -> bool:
        """Compare versions: (major, minor) tuple ordering."""
        return (self.major, self.minor) < (other.major, other.minor)

    def __le__(self, other: Self) -> bool:
        """Less than or equal comparison."""
        return (self.major, self.minor) <= (other.major, other.minor)

    def __gt__(self, other: Self) -> bool:
        """Greater than comparison."""
        return (self.major, self.minor) > (other.major, other.minor)

    def __ge__(self, other: Self) -> bool:
        """Greater than or equal comparison."""
        return (self.major, self.minor) >= (other.major, other.minor)


# Current version for ma114tsdb data formats
CURRENT_VERSION = Version(1, 0)
