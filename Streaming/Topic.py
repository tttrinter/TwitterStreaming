"""
Created in December of 2017

Class for describing a topic (currently Life Events) model in the TwitterStreaming process. A Topic
is used to save the filter terms and exclusion terms to use to evaluate Tweets.

Topics are stored in the database in the "topics" table.

@author: tom trinter
"""

from typing import List, DefaultDict
from TwitterRDS import RDSQueries as q
from .Model import Model

class Topic(object):
    """
    Static class to method to hold lists for filters, exclusions and associated models
    """

    def __init__(self, topic_id=None, name=None, filters: List[str]=[], exclusions: List[str]=[], models_list=[], description:str=None):
        self.topic_id = topic_id
        self.name = name
        self.filters = filters
        self.exclusions = exclusions
        self.models_list = models_list
        self.description = description

    def saveTopic(self, db=None):
        """ Saves the filters, exclusions and model data to the database
        Args:
            db: a database connection.

        Returns:
            status: success or failure of the save
        """
        # Insert Topic
        topic_id = q.insert_topic(self.name, self.filters, self.exclusions, self.description, db)
        for model in self.models_list:
            model.topic_id=topic_id
            try:
                model_id = model.saveModel(db=db)
                model.model_id = model_id
            except Exception as e:
                print(e)
                pass

        return topic_id


    def readTopic(self, db=None):
        """ Reads the filters, exclusions and model data from a database into the Topic
        Args:
            topic_id: int, the row for the model in the database
            db: a database connection.

        Returns:
            topic: the populated Topic object
    """
        topic_dict = q.read_topic(self.topic_id, con=db)
        if topic_dict is not None:
            for key in topic_dict:
                if key in ['filters', 'exclusions']:
                    self.__setattr__(key, [x.strip() for x in topic_dict[key].split(',')])
                else:
                    self.__setattr__(key, topic_dict[key])
        else:
            return

        models_df = q.read_topic_models(self.topic_id)
        if len(models_df) > 0 :
            cols = models_df.columns
            for index, row in models_df.iterrows():
                new_model = Model()
                for col in cols:
                    new_model.__setattr__(col, row[col])
                self.models_list.append(new_model)




