import os
import bittensor as bt
import pathlib


def init_wallet():
    # Disable bittensor logging during wallet operations (v8 uses state machine)
    try:
        bt.logging.disable_logging()
    except Exception:
        pass  # Ignore if logging state doesn't allow transition

    try:
        # Initialize wallet - always use default names
        wallet = bt.wallet(
            name="default", path=os.path.join(os.environ["HOME"], ".bittensor/wallets/")
        )

        coldkey_mnemonic = os.getenv("COLDKEY_MNEMONIC")
        if not coldkey_mnemonic:
            raise Exception("COLDKEY_MNEMONIC environment variable is required")

        # Regenerate coldkey - ignore return value since it outputs success message
        wallet.regenerate_coldkey(
            mnemonic=coldkey_mnemonic, use_password=False, overwrite=True
        )

        # Handle hotkey initialization
        hotkey_mnemonic = os.getenv("HOTKEY_MNEMONIC")
        if hotkey_mnemonic:
            # Use provided mnemonic - ignore return value
            wallet.regenerate_hotkey(
                mnemonic=hotkey_mnemonic, use_password=False, overwrite=True
            )
        elif os.getenv("AUTO_GENERATE_HOTKEY", "").lower() == "true":
            # Generate new hotkey - ignore return value
            wallet.create_new_hotkey(use_password=False, overwrite=True)
        else:
            # Check if hotkey exists, if not, we need one of the above methods
            hotkey_path = pathlib.Path(
                os.path.join(
                    os.environ["HOME"], ".bittensor/wallets/default/hotkeys/default"
                )
            )
            if not hotkey_path.exists():
                msg = (
                    "Either HOTKEY_MNEMONIC must be provided or "
                    "AUTO_GENERATE_HOTKEY must be set to true"
                )
                raise Exception(msg)
    except Exception as e:
        print(f"Error initializing wallet: {e}")
        raise e
    finally:
        # Re-enable default logging (v8 uses state machine)
        try:
            bt.logging.enable_default()
        except Exception:
            pass  # Ignore if logging state doesn't allow transition


if __name__ == "__main__":
    init_wallet()
