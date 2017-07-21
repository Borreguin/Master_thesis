Run the following in the terminal:

sudo apt-get install $(grep -vE "^\s*#" packagelist  | tr "\n" " ")

To install the HMM library go to:

https://github.com/hmmlearn/hmmlearn
