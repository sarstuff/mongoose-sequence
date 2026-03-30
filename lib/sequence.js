const mongoose = require('mongoose');
const SequenceArchive = require('./sequence_archive');

const sequenceArchive = SequenceArchive.getSingleton();
let Sequence;

/**
 * Resolve a dot-notation path against an object.
 * e.g. resolve('registration.number', doc) => doc.registration.number
 */
const resolve = (path, obj) =>
  path.split('.').reduce((prev, curr) => (prev ? prev[curr] : null), obj);

module.exports = function SequenceFactory(connection) {
  if (arguments.length !== 1) {
    throw new Error(
      'Please, pass mongoose while requiring mongoose-sequence: https://github.com/ramiel/mongoose-sequence#requiring',
    );
  }

  /**
   * Sequence plugin constructor
   *
   * @class Sequence
   * @param {object} schema   A mongoose Schema
   * @param {object} options
   * @param {string}          [options.inc_field='_id']        The field to increment.
   * @param {string}          [options.id]                     Unique id for this sequence.
   *                                                           Mandatory when using reference_fields.
   *                                                           Strongly recommended in all cases.
   * @param {string|string[]} [options.reference_fields]       Fields that scope the counter.
   * @param {boolean}         [options.disable_hooks=false]    If true the counter will not
   *                                                           auto-increment on save.
   * @param {string}          [options.collection_name='counters'] Counter collection name.
   * @param {boolean}         [options.exclusive=true]         Set false to allow sharing a
   *                                                           sequence across multiple schemas.
   * @param {number}          [options.start_seq=1]            Starting value for the sequence.
   * @param {number}          [options.inc_amount=1]           Increment step.
   */
  Sequence = function (schema, options) {
    const defaults = {
      id: null,
      inc_field: '_id',
      start_seq: 1,
      inc_amount: 1,
      reference_fields: null,
      disable_hooks: false,
      collection_name: 'counters',
      exclusive: true,
    };

    const optionsNew = { ...defaults, ...options };

    if (optionsNew.reference_fields === null) {
      optionsNew.reference_fields = optionsNew.inc_field;
      this._useReference = false;
    } else {
      this._useReference = true;
    }

    optionsNew.reference_fields = Array.isArray(optionsNew.reference_fields)
      ? optionsNew.reference_fields
      : [optionsNew.reference_fields];

    optionsNew.reference_fields = optionsNew.reference_fields.sort();

    if (this._useReference === true && optionsNew.id === null) {
      throw new Error('Cannot use reference fields without specifying an id');
    } else {
      optionsNew.id = optionsNew.id || optionsNew.inc_field;
    }

    this._options = optionsNew;
    this._schema = schema;
    this._counterModel = null;
  };

  /**
   * Create and register a Sequence instance.
   *
   * @static
   * @param {object} schema
   * @param {object} options
   * @returns {Sequence}
   */
  Sequence.getInstance = function (schema, options) {
    const sequence = new Sequence(schema, options);
    const id = sequence.getId();
    const existsSequence = sequenceArchive.existsSequence(id);

    sequence.enable();

    if (!existsSequence) {
      sequenceArchive.addSequence(id, sequence);
    } else if (sequence._options.exclusive) {
      throw new Error(`Counter already defined for field "${id}"`);
    }

    return sequence;
  };

  /**
   * Initialise the sequence: build the counter model, patch the schema.
   */
  Sequence.prototype.enable = function () {
    this._counterModel = this._createCounterModel();
    this._createSchemaKeys();
    this._setMethods();

    if (this._options.disable_hooks === false) {
      this._setHooks();
    }
  };

  /**
   * @returns {string} The id of this sequence.
   */
  Sequence.prototype.getId = function () {
    return this._options.id;
  };

  /**
   * Build the reference_value object for a given document.
   * Returns null when this sequence does not use reference fields.
   *
   * @param {object} doc  A mongoose document (or plain object for counterReset).
   * @returns {object|null}
   */
  Sequence.prototype._getCounterReferenceField = function (doc) {
    if (this._useReference === false) {
      return null;
    }

    const reference = {};
    this._options.reference_fields.forEach((field) => {
      reference[field] = resolve(field, doc);
    });
    return reference;
  };

  /**
   * Add the increment field to the schema if it is not already present.
   * Throws if the field exists but is not a Number.
   */
  Sequence.prototype._createSchemaKeys = function () {
    const schemaKey = this._schema.path(this._options.inc_field);

    if (schemaKey === undefined) {
      const fieldDesc = {};
      fieldDesc[this._options.inc_field] = 'Number';
      this._schema.add(fieldDesc);
    } else if (schemaKey.instance !== 'Number') {
      throw new Error(
        'Auto increment field already present and not of type "Number"',
      );
    }
  };

  /**
   * Build (or retrieve) the Mongoose model that backs the counters collection.
   *
   * @returns {mongoose.Model}
   */
  Sequence.prototype._createCounterModel = function () {
    const modelName = `Counter_${this._options.id}`;

    if (connection.modelNames().includes(modelName)) {
      return connection.model(modelName);
    }

    const CounterSchema = new mongoose.Schema(
      {
        id: { type: String, required: true },
        reference_value: { type: mongoose.Schema.Types.Mixed, required: true },
        seq: { type: Number, required: true },
      },
      {
        collection: this._options.collection_name,
        validateBeforeSave: false,
        versionKey: false,
        _id: false,
      },
    );

    CounterSchema.index({ id: 1, reference_value: 1 }, { unique: true });

    return connection.model(modelName, CounterSchema);
  };

  /**
   * Register the pre-save hook on the schema.
   * Only increments on new documents.
   */
  Sequence.prototype._setHooks = function () {
    const sequence = this;

    this._schema.pre('save', async function () {
      if (!this.isNew) return;
      const seq = await sequence._getNextSequence(this);
      this.set(sequence._options.inc_field, seq);
    });
  };

  /**
   * Attach instance method setNext and static counterReset to the schema.
   */
  Sequence.prototype._setMethods = function () {
    this._schema.method('setNext', async function (id) {
      const sequence = sequenceArchive.getSequence(id);

      if (sequence === null) {
        throw new Error(
          `Trying to increment a wrong sequence using the id ${id}`,
        );
      }

      const seq = await sequence._getNextSequence(this);
      this.set(sequence._options.inc_field, seq);
      return this.save();
    });

    this._schema.static('counterReset', async function (id, reference) {
      const sequence = sequenceArchive.getSequence(id);
      return sequence._resetCounter(id, reference);
    });
  };

  /**
   * Atomically retrieve the next sequence value for a document.
   *
   * Uses an aggregation pipeline update so the insert and increment are a
   * single round-trip. On first insert $seq is missing, so $ifNull returns
   * start_seq. On subsequent calls it returns $seq + inc_amount.
   *
   * Requires MongoDB 5.0+ (guaranteed by Mongoose 8 peer dependency).
   *
   * @param {object} doc  A mongoose document.
   * @returns {Promise<number>}
   */
  Sequence.prototype._getNextSequence = async function (doc) {
    const id = this.getId();
    const referenceValue = this._getCounterReferenceField(doc);
    const { start_seq, inc_amount } = this._options;

    const counter = await this._counterModel.findOneAndUpdate(
      { id, reference_value: referenceValue },
      [
        {
          $set: {
            seq: {
              $ifNull: [{ $add: ['$seq', inc_amount] }, start_seq],
            },
          },
        },
      ],
      { new: true, upsert: true },
    );

    return counter.seq;
  };

  /**
   * Reset the counter(s) for a given sequence id.
   *
   * @param {string}  id         The sequence id to reset.
   * @param {object}  [reference] If provided, resets only the counter for that
   *                              specific reference value. Omit to reset all
   *                              counters for the id.
   * @returns {Promise}
   */
  Sequence.prototype._resetCounter = async function (id, reference) {
    const condition = { id };
    const seq = this._options.start_seq ? this._options.start_seq - 1 : 0;

    if (reference !== undefined && !(reference instanceof Function)) {
      condition.reference_value = this._getCounterReferenceField(reference);
    }

    return this._counterModel.updateMany(condition, { $set: { seq } });
  };

  return Sequence.getInstance;
};
