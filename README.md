# rabbitManager
Created to manage messages in rabbitMQ.

Important: Initially the instructions listed here are only applicable to Linux based on the Debian distribution. If you are using another operating system, adapt it yourself. A docker container will be added in the future to test the script functionalities.


# General Resources and usage scenarios
Reading:
    Read messages with implicity JSON format.
    Save backup copies of individuaal messages.
    Read messages one by one and make ACK.
    Generate a JSON list with all messages in the queue.
Posting:
    Post messages in a queue from a JSON list.


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

Reading all messages and writing a payloads.json list in current directory:

    python3 rabbitManager.py [queue-name] backup

Reading all messages and writing a payloads.json list in a new directory nested with the queue's name [queue-name] (removing messages from queue)

    python3 rabbitManager.py [queue-name] backup --auto-ack

Reading all messages and writing a payloads.json list and individual file messages in a new directory nested with the queue's name [queue-name]

    python3 rabbitManager.py [queue-name] backup --one-file-per-msg


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
	Include confirmation message when user call method backup without --auto-ack parameter. 
	Add a new script file to automated the debian package creation process.
	Add a docker-compose to up a container rabbitMQ to test the script.

