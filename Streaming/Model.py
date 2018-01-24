"""
Created sometime in December of 2017

Class for describing a model in the TwitterStreaming process. A model
is used to evaluate a downloaded tweet for the occurance of a specific life event.
The life event is defined as a Topic - see the Topic class for more info.

In addition to descriptive information for each model, it also contains:
1. file name of the pickled sci-kit learn model (this could be extended to other models in the future)
2. vectorizer - the name of the pickled vectorizer that matched the model for featurization
3. model_path - where the model and vectorizer are saved.

All of this data is saved in the database in the "models" table.

@author: tom trinter
"""


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
        """ Saves the model terms to the database. This currently assumes the model does not exist.
        Args:
            db: a database connection.

        Returns:
            model_id: the new id of the model saved to the database
        """

        # Insert Model
        # TODO: check for existing model and exit (or update) when found
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
            name:
            db: a database connection.

        Returns:
            topic: the populated Topic object
"""
        ...


    def updateModel(self, db=None):
        ...