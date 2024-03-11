package com.mongodb.socialite.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.bson.Document;

/**
 * Base class for binding MongoDB Documents to user data classes with
 * Jackson annotations. Can be initialized with documents returned from
 * MongoDB queries and returned directly via a dropwizard service without
 * exposing all document content (only those properties defined and
 * annotated with @JsonProperty in the derived class will be extracted)
 *
 */
public abstract class MongoDataObject {

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof MongoDataObject))
			return false;

		MongoDataObject rhs = (MongoDataObject) obj;
		return this._document.equals(rhs._document);
	}

	@Override
	public int hashCode() {
		return _document.hashCode();
	}

	@Override
	public String toString() {
		return _document.toString();
	}

	protected Document _document = null;

	public MongoDataObject(Document document){
		if(document != null)
			_document = document;
		else
			_document = new Document();
	}

	public MongoDataObject(){
		_document = new Document();
	}

	@JsonIgnore
	public Document toDocument(){
		return _document;
	}
}