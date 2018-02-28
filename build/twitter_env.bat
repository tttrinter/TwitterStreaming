setx path "%path%;c:\Users\%USERNAME%\Anaconda3\Scripts\"
conda create -n twitter python=3.6 --yes
activate twitter
pip install tweepy
pip install boto3
conda install -y pandas
pip install psycopg2
conda install -y numba
conda install -y scikit-learn
conda install -y nltk
conda install -y pytables
conda install -y snappy
conda install -y sqlalchemy