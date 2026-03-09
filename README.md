# rabbitManager
Created to manage messages in rabbitMQ.

Important: Initially the instructions listed here are only applicable to Linux based on the Debian distribution. If you are using another operating system, adapt it yourself. A docker container will be added in the future to test the script functionalities.


# General Resources and usage scenarios
Reading:
    Read messages with implicity JSON format.
    Save backup copies in rotating .jsonl files.
    Read messages one by one and make ACK.
Posting:
    Post messages in a queue from a JSON list.
    Post messages in a direct, topic or fanout exchange.


# Requirements to develop and test
Created and tested in Python version 3.10.12

sudo apt update
sudo apt install python3
sudo apt install python3-pip
pip3 install pika

# Running the script

Open your linux terminal:

After python 3 installation, declare rabbitMQ connection environment variables:

    export RABBITMQ_VHOST=[vhost-name]
    export RABBITMQ_HOST=[hostname]
    export RABBITMQ_USER=[username]
    export RABBITMQ_PASSWD=[user_password]
    export RABBITMQ_PORT=5672

Reading messages one by one

    python3 rabbitManager.py [queue-name]

Backup with rotating .jsonl files and ACK per message after persistence confirmed

    python3 rabbitManager.py [queue-name] backup

Backup with custom batch size and prefetch

    python3 rabbitManager.py [queue-name] backup --batch-size 1000 --prefetch-count 200

Backup with batch ACK
Important: the script asks for an explicit confirmation before consuming the queue

    python3 rabbitManager.py [queue-name] backup --ack-mode batch

Backup without ACK (use with extreme caution; ensure the queue has around 100 messages)
The script will emit a red alert before starting and wait for confirmation.

    python3 rabbitManager.py [queue-name] backup --no-ack

Backup with rotating .jsonl files and individual message files

    python3 rabbitManager.py [queue-name] backup --one-file-per-msg

Posting messages in a queue from a JSON list

    python3 rabbitManager.py [queue-name] [json-file]

Posting messages in a direct exchange with routing key

    python3 rabbitManager.py --exchange [exchange-name] --exchange-type direct --routing-key [routing-key] [json-file]

Posting messages in a topic exchange with routing key

    python3 rabbitManager.py --exchange [exchange-name] --exchange-type topic --routing-key [routing-key] [json-file]

Posting messages in a fanout exchange

    python3 rabbitManager.py --exchange [exchange-name] --exchange-type fanout [json-file]


Help:

    python3 rabbitManager.py

# Requirements to generate a executable and distribuition debian package (.deb)
This will be needed to generate a Debian Package (DEB):

    sudo pip install pyinstaller
    sudo apt-get install dh-make devscripts build-essential

Run this command to create a executable file with the same name of script. The file Will be created in a nested directory called "dist"

    pyinstaller --onefile rabbitManager.py

Copy (or rewrite) the executable file to [project-dir]/usr/local/bin

    cp dist/rabbitManager usr/local/bin    

Generate the debian package (.deb) outside the project root directory [project-dir]

    cd ..
    dpkg-deb --build rabbitManager

Increment version in DEBIAN/control file

    version: 1.0 --> 1.1 or 2.0


# Futures releases and new features

	Include header with message_id when downloading json individual files and when list json payload output file. 
	Add a new script file to automated the debian package creation process.
	Add a docker-compose to up a container rabbitMQ to test the script.

