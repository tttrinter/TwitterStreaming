rem install the python Anaconda distribution: https://www.anaconda.com/download/#download

setx path "%path%;c:\Users\%USERNAME%\Anaconda3\Scripts\"
conda create -n twitter python=3.6 --yes
activate twitter
pip install tweepy
pip install boto3
conda install -y pandas
pip install psycopg2
pip install awscli --upgrade --user

rem modify ~/.aws/configuration - to include a [di] profile and a recongnized key and secret:
rem    [di]
rem    aws_access_key_id = AKIAIZU6W5D4L34KMI4Q
rem    aws_secret_access_key = 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
rem    region=us-east-1

setx path "%path%;c:\Users\%USERNAME%\AppData\Roaming\Python\Python36\Scripts"
conda install -y numba
conda install -y scikit-learn
conda install -y nltk
conda install -y pytables
conda install -y snappy
conda install -y sqlalchemy

rem add the security group from the EC2 machine to the inbound rules for RDS
rem create an SSH key for CodeCommit from the EC2 machine and add it to a user for CodeCommit
rem save the rsa and res.pub files to the ~/.ssh/ directory
rem modify the .ssh/config file:
rem Host git-codecommit.us-east-1.amazonaws.com
rem  User [new SSH key id from IAM]
rem  IdentityFile ~/.ssh/[rsa file name]

rem Copy models from https://drive.google.com/drive/folders/1htCJL_-2pCQZpc0DPqe0HhSpXFbWhdHQ to C:\Users\Administrator\Documents\Clients\Thrivent\Twitter\Life Events\