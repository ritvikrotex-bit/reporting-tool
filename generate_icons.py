"""
generate_icons.py — creates static/icon-192.png and static/icon-512.png
using only Python stdlib (no Pillow required).
Run once: python generate_icons.py
"""

import os
import struct
import zlib


def _make_chunk(chunk_type: bytes, data: bytes) -> bytes:
    c = chunk_type + data
    return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)


def create_png(size: int, bg: tuple, fg: tuple, out_path: str):
    """
    Create a solid-colour square PNG with a simple 'R' letter mark.
    bg = (r,g,b) background colour
    fg = (r,g,b) foreground colour for the letter
    """
    w = h = size
    pixels = []

    # Simple 5x7 pixel font for 'R' scaled to ~20% of icon size
    letter_r = [
        [1, 1, 1, 1, 0],
        [1, 0, 0, 0, 1],
        [1, 0, 0, 0, 1],
        [1, 1, 1, 1, 0],
        [1, 0, 1, 0, 0],
        [1, 0, 0, 1, 0],
        [1, 0, 0, 0, 1],
    ]

    cell = max(size // 10, 2)
    ox = (size - 5 * cell) // 2
    oy = (size - 7 * cell) // 2

    def is_letter(x, y):
        col = (x - ox) // cell
        row = (y - oy) // cell
        if 0 <= row < 7 and 0 <= col < 5:
            return letter_r[row][col] == 1
        return False

    raw = b""
    for y in range(h):
        raw += b"\x00"  # filter type: None
        for x in range(w):
            if is_letter(x, y):
                raw += bytes(fg)
            else:
                raw += bytes(bg)

    sig   = b"\x89PNG\r\n\x1a\n"
    ihdr  = _make_chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
    idat  = _make_chunk(b"IDAT", zlib.compress(raw, 9))
    iend  = _make_chunk(b"IEND", b"")

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(sig + ihdr + idat + iend)
    print(f"  Created {out_path} ({size}x{size}px)")


if __name__ == "__main__":
    BG = (11, 18, 32)    # #0b1220 — dark navy
    FG = (59, 130, 246)  # #3b82f6 — blue
    create_png(192, BG, FG, "static/icon-192.png")
    create_png(512, BG, FG, "static/icon-512.png")
    print("Done.")
