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

from io import BytesIO

from dulwich.objects import ShaFile
from dulwich.object_store import MemoryObjectStore
from dulwich.pack import PackData, PackInflater
from dulwich.refs import DictRefsContainer
from dulwich.repo import BaseRepo

from pymongo.mongo_client import MongoClient


class MongoObjectStore(MemoryObjectStore):
    """ Object Store that stores loose objects in a MongoDB collection. 
    This class is subclassed from MemoryObjectStore because the
    MongoObjecStore does not deal with packs just as the MemoryObjectStore.
    """
    
    def __init__(self, db):
        """ Initialize the Object Store with a connection to a mongo
        database instance.
        
        :param db: A Mongo Database instance.
        """
        super(MongoObjectStore, self).__init__()
        self.coll = 'objects'
        self.db = db

    def __repr__(self):
        """String name for this object."""
        return "<%s(%r)>" % (self.__class__.__name__, self.coll_name)

    def __iter__(self):
        """ Return an iterator of all id/shas in the collection. """
        return iter(
            [i.get('_id') 
                for i in self.db[self.coll].find({}, {'_id': True})
            ]
        )

    def __getitem__(self, name):
        """ Add __getitem__ to provide key access to the object store. """
        entry = self.db[self.coll].findOne({'_id': self._to_hexsha(sha)})
        if not entry:
            raise KeyError(name)

        return self._data_to_obj(entry)

    def _data_to_obj(self, db_entry):
        """ Given a mongo document, return the deserialized object by
        its stored type.
        
        :param db_entry: A MongoDB document.
        """
        return ShaFile.from_raw_string(
            db_entry.get('type'),
            db_entry.get('data'),
            db_entry.get('_id')
        )

    def get_raw(self, name):
        """Obtain the raw text for an object.

        :param name: sha for the object.
        :return: tuple with numeric type and object contents.
        """
        # Instead of deserializing an object via __getitem__ just return
        # the values in the database.
        entry = self.db[self.coll].findOne({'_id': self._to_hexsha(sha)})
        if not entry:
            return None, None

        return entry.get('type'), entry.get('data') 

    def contains_loose(self, sha):
        """ Determine if the requested sha exists in this store.
        
        :param sha: The SHA to check for.
        :return: boolean
        """
        return bool(
            self.db[self.coll].findOne({'_id': self._to_hexsha(sha)})
        )

    def add_object(self, obj):
        """ Add object to the collection.
        
        :param obj: The Object to add to the store.
        """
        self.db[self.coll].insert({
            '_id': obj.id,
            'type': obj.type_num,
            'data': str(obj)
        })

    def add_objects(self, objects):
        """ Add multiple objects to the collection.

        :param objects: An iterable of tuples of object and path.
        """
        docs = [{
                '_id': obj.id,
                'type': obj.type_num,
                'data': str(obj)
            } for obj, path in objects]

        self.db[self.coll].insert(docs)

    def add_pack(self):
        """Add a new pack to this object store.

        Because this object store doesn't support packs, we extract and add the
        individual objects.
        :return: Fileobject to write to and a commit function to call when the
            pack is finished.
        """
        f = BytesIO()

        def commit():
            p = PackData.from_file(BytesIO(f.getvalue()), f.tell())
            f.close()
            self.add_objects(
                for obj in PackInflater.for_pack_data(p, self.get_raw)
            )
        
        def abort():
            pass

        return f, commit, abort


class MongoRefsContainer(DictRefsContainer):
    """ A refs container that stores loose refs in a MongoDB collection. 
    This class is subclassed from DictRefsContainer because the
    MongoRefsContainer does not deal with symbolic or packed references.
    """

    def __init__(self, db):
        """ Initialize the Refs Container with a connection to a mongo
        database instance.
        
        :param db: A Mongo Database instance.
        """
        self.coll = 'refs'
        self.db = db


class MongoRepo(BaseRepo):
    pass
