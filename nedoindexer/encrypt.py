import struct
import base64


def ton_crc16(data: bytes) -> bytes:
    """Compute CRC16 using TON specific polynomial."""
    poly = 0x1021
    reg = 0
    message = bytearray(data)
    for byte in message:
        reg ^= byte << 8
        for _ in range(8):
            if reg & 0x8000:
                reg = (reg << 1) ^ poly
            else:
                reg <<= 1
            reg &= 0xFFFF
    return struct.pack('>H', reg)


def convert_raw_to_user_friendly(raw_address: str) -> dict:
    # Split workchain_id and address
    workchain_id_str, address_str = raw_address.split(':')
    workchain_id = int(workchain_id_str)
    raw_bytes = bytes.fromhex(address_str)

    if len(raw_bytes) != 32:
        raise ValueError("Invalid raw address length, must be 32 bytes.")

    def generate_address(is_bounceable: bool) -> str:
        # Determine the first byte: 
        # - 0x11 for bounceable
        # - 0x51 for non-bounceable
        flag_byte = 0x11 if is_bounceable else 0x51

        # Combine the flag byte, workchain_id and raw address
        address_with_flags = bytes([flag_byte, workchain_id & 0xFF]) + raw_bytes

        # Compute checksum
        checksum = ton_crc16(address_with_flags)

        # Combine the address and checksum
        address_with_checksum = address_with_flags + checksum

        # Encode to base64 (URL-safe)
        user_friendly_address = base64.urlsafe_b64encode(address_with_checksum).decode('utf-8')

        # Remove any trailing '=' characters used for padding in base64
        user_friendly_address = user_friendly_address.rstrip('=')

        return user_friendly_address

    bounceable = generate_address(is_bounceable=True)
    non_bouceable = generate_address(is_bounceable=False)

    return bounceable, non_bouceable
