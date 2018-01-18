import TwitterRDS.RDSQueries as q

class Model(object):
    """
    Static class to method to hold model details
    """

    def __init__(self,
                 name:str=None,
                 type: str=None,
                 model_id=None,
                 topic_id=None,
                 description=None,
                 filename:str=None,
                 vectorizer:str=None,
                 model_path:str=None):

        self.name = name
        self.type = type
        self.model_id = model_id
        self.topic_id = topic_id
        self.description = description
        self.filename = filename
        self.vectorizer = vectorizer
        self.model_path = model_path

    def saveModel(self, db=None):
        """ Saves the model terms to the database
        Args:
            db: a database connection.

        Returns:
            status: success or failure of the save
        """
        # Insert Model
        model_id = q.insert_model(name=self.name,
                                  type=self.type,
                                  topic_id=self.topic_id,
                                  description=self.description,
                                  filename=self.filename,
                                  vectorizer=self.vectorizer,
                                  model_path=self.model_path,
                                  con=db)

        return model_id


    def readModel(self, name, db=None):
        """ Reads the filters, exclusions and model data from a database into the Topic
        Args:
            db: a database connection.

        Returns:
            topic: the populated Topic object
"""
        ...


    def updateModel(self, db=None):
        ...