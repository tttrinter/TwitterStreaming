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
import logging

class Topic(object):
    """
    Static class to method to hold lists for filters, exclusions and associated models
    """

    def __init__(self,
                 topic_id=None,
                 name=None,
                 filters: List[str]=[],
                 exclusions: List[str]=[],
                 models_list=[],
                 description:str=None,
                 threshold:float=0.5,
                 sub_topics: List[int]=[]):

        self.topic_id = topic_id
        self.name = name
        self.filters = filters
        self.exclusions = exclusions
        self.models_list = models_list
        self.description = description
        self.threshold = threshold
        self.sub_topics = sub_topics

    def saveTopic(self, db=None):
        """ Saves the filters, exclusions and model data to the database
        Args:
            db: a database connection.

        Returns:
            status: success or failure of the save
        """
        # Insert Topic
        topic_id = q.insert_topic(self.name, self.filters, self.exclusions, self.description, self.threshold, self.sub_topics, db)
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
        logging.info("readTopic.")
        topic_dict = q.read_topic(self.topic_id, con=db)
        logging.info("readTopic: topicDict: {0}".format(topic_dict))
        if topic_dict is not None:
            for key in topic_dict:
                if key in ['filters', 'exclusions']:
                    if topic_dict[key] is not None:
                        self.__setattr__(key, [x.strip() for x in topic_dict[key].split(',')])
                elif key == 'sub_topics':
                    if topic_dict[key] is not None:
                        self.__setattr__(key, [int(x.strip()) for x in topic_dict[key].split(',')])
                else:
                    self.__setattr__(key, topic_dict[key])
        else:
            logging.info("readTopic: topicDict is none.")
            return

        # For some reason the model list was lingering from previous topics - clear it out if necessary
        if len(self.models_list) > 0:
            self.models_list=[]
        models_df = q.read_topic_models(self.topic_id)
        if models_df is None:
            return
        elif len(models_df) > 0:
            cols = models_df.columns
            for index, row in models_df.iterrows():
                new_model = Model()
                for col in cols:
                    new_model.__setattr__(col, row[col])
                self.models_list.append(new_model)
            return




