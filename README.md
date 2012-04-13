# Knowledge Acquisition for Next Generation Statement Map

Author: Eric Nichols, <eric@ecei.tohoku.ac.jp>

## Tools

* instances2matrix.py: creates a matrix of co-occurence counts between relation 
  pattern x arguments in mongodb from input instances

### Instance Format

Instances have the following tab-delimited format:

* score: score representing weight * co-occurence count for instance
* loc: giving source and location of instance
* rel: containing relation pattern
* argc: giving argument count
* argv: tab-delimited list of arguments as strings

#### Example

    1.0\treverb_clueweb_tuples-1.1.txt:30:10-11\tARG1 acquired ARG2\t2\Google\tYouTube
     
### Matrix Database Format
     
The co-occurence matrix has the following fields:
     
* rel: relation pattern
* arg1: first argument
* ...
* argn: nth argument
     
It is indexed for fast look up of rel, args, and (rel,args) tuples.
