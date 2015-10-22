# -*- coding: utf-8 -*-
# mongo.py -- A Repo, Object Store and Refs implementation backing into a 
# MongoDB database.
# Copyright 2015 Nathen Hinson <nathen.hinson@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


""" A Repo, Object Store and Refs implementation backing into a MongoDB 
database.
"""

from dulwich.object_store import MemoryObjectStore
from dulwich.repo import BaseRepo

from pymongo.mongo_client import MongoClient


class MongoObjectStore(MemoryObjectStore):
    """ Object Store that stores loose objects in a MongoDB collection. 
    This class is subclassed from MemoryObjectStore because the
    MongoObjecStore does not deal with packs just as the MemoryObjectStore.
    """
    
    def __init__(self, reponame, mongodb_uri):
        """ Initialize the Object Store with a connection to a mongo
        database instance.
        
        :param reponame: The name of the repository, used as the collection
        name.
        :param mongodb_uri: A valid MongoDB URI used to create a MongoClient
        instance. Errors will bubble up.
        """
        super(MongoObjectStore, self).__init__()
        self.db_name = reponame
        self.obj_coll = 'objects'
        self.refs_coll = 'refs'
        self.conn = MongoClient(mongodb_uri)
        self.db = self.conn.get_database(self.db_name)

    def __repr__(self):
        """String name for this object."""
        return "<%s(%r)>" % (self.__class__.__name__, self.coll_name)

    def contains_loose(self, sha):
        return bool(
            self.db[self.obj_coll].findOne({'_id': self._to_hexsha(sha)})
        )


class MongoRepo(BaseRepo):
    pass
