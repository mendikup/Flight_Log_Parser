from src.pipeline.parallel_bin_decoder import ParallelBinDecoder
from src.config.config_loader import config

def main() -> None:
    """
    Main entry point for the high-performance ArduPilot BIN log decoder.
    Initializes the parallel decoder and runs the extraction workflow.
    """
    log_file_path = config.general.log_file_path

    decoder = ParallelBinDecoder(
        file_path=log_file_path,
        num_workers=8,
        round_floats=True,
        running_mode="process",
        message_filter={"GPS"},
    )

    decoded_messages = decoder.run()

    for message in decoded_messages[:10]:
        print(message)


if __name__ == "__main__":
    main()