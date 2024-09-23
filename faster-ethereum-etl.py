import subprocess
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

# Set up logging
logging.basicConfig(
    filename="export_blocks.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Define the start and end block

start_block = 0
end_block = 20000000
step = 10000  # Process 10,000 blocks at a time

# Set the Ethereum node URI

provider_uri = "http://127.0.0.1:8545"


# Define the task that each process will execute


def export_blocks(start_block, end_block):
    # Generate output file names
    blocks_output = f"blocks/blocks_{start_block}_{end_block}.csv"
    transactions_output = f"transactions/transactions_{start_block}_{end_block}.csv"

    # Construct the command
    command = [
        "ethereumetl",
        "export_blocks_and_transactions",
        f"--start-block={start_block}",
        f"--end-block={end_block}",
        "--batch-size=20",
        "-w",
        "1",
        f"--provider-uri={provider_uri}",
        f"--blocks-output={blocks_output}",
        f"--transactions-output={transactions_output}",
    ]

    # Print and log the command being executed
    command_str = " ".join(command)
    print(f"Executing: {command_str}")
    logging.info(f"Executing: {command_str}")

    # Execute the command and capture stdout and stderr in real-time
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # Treat output as text instead of bytes
            bufsize=1,  # Line buffering
        )

        # Monitor stdout and stderr in real-time
        with process.stdout, process.stderr:
            for line in process.stdout:
                print(line, end="")  # Print to console
                logging.info(line.strip())  # Write to log file

            for line in process.stderr:
                print(line, end="")  # Print to console
                logging.error(line.strip())  # Write to log file

        # Wait for the subprocess to complete
        process.wait()

        # Check the return code of the subprocess
        if process.returncode != 0:
            logging.error(f"Command failed with exit code {process.returncode}")
            print(f"Command failed with exit code {process.returncode}")
        else:
            logging.info(
                f"Command completed successfully for blocks {start_block} to {end_block}"
            )
            print(
                f"Command completed successfully for blocks {start_block} to {end_block}"
            )

    except Exception as e:
        logging.error(f"Error while executing command: {e}")
        print(f"Error while executing command: {e}")


# Use a process pool for concurrent execution
def main():
    max_workers = (
        64  # Number of concurrent processes, adjust based on system performance
        #! Notice that if the Ethereum node is local, workers will bring extra
        #! workload to both the node and the crawler.
    )

    # Create a process pool
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = []
        for i in range(start_block, end_block, step):
            current_end_block = min(
                i + step - 1, end_block
            )  # Ensure we don't exceed the `end_block`
            futures.append(executor.submit(export_blocks, i, current_end_block))

        # Wait for all tasks to complete and handle results
        for future in as_completed(futures):
            try:
                future.result()  # Check if the task raised any exceptions
            except Exception as e:
                logging.error(f"Error in task execution: {e}")
                print(f"Error in task execution: {e}")

    print("All blocks have been exported!")


if __name__ == "__main__":
    main()
