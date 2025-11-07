from src.bussines_logic.controller import ParallelBinDecoder
from src.utils.utils import logger

def main():
    log_file = "log_file_test_01.bin"

    decoder = ParallelBinDecoder(
        file_path=log_file,
        num_workers=8,
        round_floats=True,
        running_mode= "process",
        message_filter= None
    )

    messages = decoder.run()
    logger.debug(f"Total messages: {len(messages)}")
    if messages:
        logger.debug(f" First message: {messages[0]}")

if __name__ == "__main__":
    main()