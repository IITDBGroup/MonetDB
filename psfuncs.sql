--------------------------------------------------------------------------------
-- BIT OR
--------------------------------------------------------------------------------
-- bitor with bigint result (up to 64 bits)
CREATE AGGREGATE bit_or_bigint(input BIGINT)
      RETURNS BIGINT
      LANGUAGE C {
          // initialize aggregate result
          result->initialize(result, aggr_group.count);

          // set bitsets to 0
          memset(result->data, 0, result->count *sizeof(result->null_value));

          for(size_t i = 0; i < input.count; i++) {
              result->data[aggr_group.data[i]] |= input.data[i];
          }
      };

-- bitor blob as variable length bitvector
CREATE AGGREGATE bit_or_blob(input BLOB)
       RETURNS BLOB
       LANGUAGE C {
           #include <string.h>

           // initialize aggregate result
           result->initialize(result, aggr_group.count);

           // set sizes (max sizes)
           for(size_t i = 0; i < input.count; i++) {
               size_t grp = aggr_group.data[i];
               result->data[grp].size = (input.data[i].size > result->data[grp].size) ?
                   input.data[i].size : result->data[grp].size;
           }

           // create initial bitsets (BLOB)
           for(size_t i = 0; i < result->count; i++) {
               result->data[i].data = malloc(result->data[i].size);
               memset(result->data[i].data, 0, result->data[i].size);
           }

           // bit or all sets together
           for(size_t i = 0; i < input.count; i++) {
               size_t grp = aggr_group.data[i];
               size_t or_size = (result->data[grp].size > input.data[i].size) ? input.data[i].size : result->data[grp].size;
               size_t words = or_size / sizeof(int64_t);
               size_t leftover = or_size % sizeof(int64_t);
               for(size_t j = 0; j < words; j++) {
                   ((int64_t *) result->data[grp].data)[j] |= ((int64_t *) input.data[i].data)[j];
               }
               size_t offset = words * sizeof(int64_t);
               for(size_t j = offset; j < offset + leftover; j++) {
                   ((char *) result->data[grp].data)[j] |= ((char *) input.data[i].data)[j];
               }
           }
       };

-- bitor that takes integer positions as input
CREATE AGGREGATE bit_or_ints(input INT)
         RETURNS BLOB
       LANGUAGE C {
       #include <string.h>

           // initialize aggregate result
           result->initialize(result, aggr_group.count);

           // set sizes (max sizes)
           for(size_t i = 0; i < input.count; i++) {
               size_t grp = aggr_group.data[i];
               size_t ssize = (input.data[i] / 8) + 1;
               result->data[grp].size = (ssize >  result->data[grp].size) ?
                   ssize : result->data[grp].size;
           }

           // create initial bitsets (BLOB)
           for(size_t i = 0; i < result->count; i++) {
               result->data[i].data = malloc(result->data[i].size);
               memset(result->data[i].data, 0, result->data[i].size);
           }

           // bit or all sets together
           for(size_t i = 0; i < input.count; i++) {
               size_t grp = aggr_group.data[i];
               size_t pos = input.data[i] / 8;
               size_t thebit = input.data[i] % 8;
               ((char *) result->data[grp].data)[pos] |= 1 << thebit;
           }
       };


--------------------------------------------------------------------------------
-- SINGLETONS
--------------------------------------------------------------------------------

-- create singleton bitset as bigint (64 bits max)
CREATE FUNCTION bitset_singleton_bigint(input INT)
   RETURNS BIGINT
   LANGUAGE C {
       result->initialize(result, input.count);

       for (size_t i = 0; i < input.count; ++i) {
           if (input.is_null(input.data[i])) {
               result->data[i] = result->null_value;
           } else {
               result->data[i] = 1 << input.data[i];
           }
       }
   };


-- create singleton bitset as blob
CREATE FUNCTION bitset_singleton_blob(input INT)
   RETURNS BLOB
   LANGUAGE C {
       #include <string.h>
       result->initialize(result, input.count);

       for (size_t i = 0; i < input.count; ++i) {
           if (input.is_null(input.data[i])) {
               result->data[i] = result->null_value;
           } else {
               size_t num_bytes = (input.data[i] / 8) + 1;
               char *result_blob = malloc(num_bytes);
               memset(result_blob, 0, num_bytes);
               result_blob[num_bytes - 1] = 1 << (input.data[i] % 8);
               result->data[i].size = num_bytes;
               result->data[i].data = result_blob;
           }
       }
   };

--------------------------------------------------------------------------------
-- TEXT->BITVECTOR and BITVECTOR->TEXT
--------------------------------------------------------------------------------
CREATE FUNCTION bitvec_to_string(input BLOB)
  RETURNS TEXT
  LANGUAGE C {
      result->initialize(result, input.count);

      for (size_t i = 0; i < input.count; ++i) {
          if (input.is_null(input.data[i])) {
              result->data[i] = result->null_value;
          } else {
              size_t length = input.data[i].size * 8;
              result->data[i] = malloc(length + 1);
              for(size_t j = 0; j < length; j++) {
                  size_t byt = (j / 8);
                  int8_t mask = 1 << (j % 8);
                  char is_on = (((char *) input.data[i].data)[byt] & mask) ? '1' : '0';
                  result->data[i][j] = is_on;
              }
              result->data[i][length] = '\0';
          }
      }
  };


CREATE FUNCTION nice_bitvec_to_string(input BLOB)
      RETURNS TEXT
      LANGUAGE C {
      result->initialize(result, input.count);

      for (size_t i = 0; i < input.count; ++i) {
          if (input.is_null(input.data[i])) {
              result->data[i] = result->null_value;
          } else {
              size_t length = input.data[i].size * 8;
              size_t strlen = input.data[i].size * 9 - 1;
              result->data[i] = malloc(strlen + 1);
              size_t pos = 0;
              for(size_t j = 0; j < length; j++) {
                  size_t byt = (j / 8);
                  if (byt > 0 && !(j % 8)) {
                      result->data[i][pos++] = ' ';
                  }
                  int8_t mask = 1 << (j % 8);
                  char is_on = (((char *) input.data[i].data)[byt] & mask) ? '1' : '0';
                  result->data[i][pos++] = is_on;
              }
              result->data[i][strlen] = '\0';
          }
      }
  };

CREATE FUNCTION string_to_bitvec(input STRING)
  RETURNS BLOB
  LANGUAGE C {
      #include <string.h>
      result->initialize(result, input.count);

      for (size_t i = 0; i < input.count; ++i) {
          if (input.is_null(input.data[i])) {
              result->data[i] = result->null_value;
          } else {
              size_t size = strlen(input.data[i]) / 8 + 1;
              result->data[i].size = size;
              result->data[i].data = malloc(size * sizeof(char));
              memset(result->data[i].data, '\0', size);
              char *d = (char *) result->data[i].data;
              for(size_t j = 0; j < strlen(input.data[i]); j++) {
                  size_t byt = j / 8;
                  size_t pos = j % 8;
                  if (input.data[i][j] == '1') {
                      d[byt] |= 1 << pos;
                  }
              }
          }
      }
  };


--------------------------------------------------------------------------------
-- BINARY SEARCH
--------------------------------------------------------------------------------
-- take ranges as input (blob consisting of int64_t values) and return position of val
CREATE FUNCTION binsearch_ranges_pos(val BIGINT, ranges BLOB)
      RETURNS INTEGER
      LANGUAGE C {
          // initialize result
          result->initialize(result, val.count);

          // make sure the second argument is a constant  and is a valid encoding of a list of ranges (a concatenation of a even length list of int64_t integers)
          if(ranges.count != 1)
              return "Second argument should be a constant!";
          if((ranges.data[0].size % 16) != 0)
              return "Second argument should be a blob storing a list of int64_t pairs as ranges";

          int64_t *r = (int64_t *) ranges.data[0].data;
          size_t num_ranges = ranges.data[0].size / 8;

          size_t low = 0;
          size_t high = num_ranges;
          size_t middle;

          for(size_t i = 0; i < val.count; i++) {
              if (val.is_null(val.data[i])) {
                  result->data[i] = result->null_value;
              } else {
                  int64_t v = (int64_t) val.data[i];
                  int64_t cur;
                  low = 0;
                  high = num_ranges;

                  while(low < high) {
                      middle = low + ((high - low) / 2);
                      cur = r[middle];
                      low = (v >= cur) ? middle + 1 : low;
                      high = (v >= cur) ? high : middle;
                  }
                  result->data[i] = low - 1;
              }
          }
      };

-- take list of intervals as input (blob consisting of int64_t value pairs) and return true if val is contained in any of the intervals
       CREATE FUNCTION binsearch_ranges_contains_val(val BIGINT, ranges BLOB)
       RETURNS BOOLEAN
       LANGUAGE C {
           // initialize result
           result->initialize(result, val.count);

           // make sure the second argument is a constant  and is a valid encoding of a list of ranges (a concatenation of a even length list of int64_t integers)
           if(ranges.count != 1)
               return "Second argument should be a constant!";
           if((ranges.data[0].size % 16) != 0)
               return "Second argument should be a blob storing a list of int64_t pairs as ranges";

           int64_t *r = (int64_t *) ranges.data[0].data;
           size_t num_ranges = ranges.data[0].size / 8;

           size_t low = 0;
           size_t high = num_ranges;
           size_t middle;

           for(size_t i = 0; i < val.count; i++) {
               if (val.is_null(val.data[i])) {
                   result->data[i] = result->null_value;
               } else {
                   int64_t v = (int64_t) val.data[i];
                   int64_t cur;
                   low = 0;
                   high = num_ranges;

                   while(low < high) {
                       middle = low + ((high - low) / 2);
                       cur = r[middle];
                       low = (v >= cur) ? middle + 1 : low;
                       high = (v >= cur) ? high : middle;
                   }
                   result->data[i] = low % 2;
               }
           }
       };

-- convenience function that turn a list of integers into a single blob
      CREATE AGGREGATE bounds_to_blob(input INTEGER)
      RETURNS BLOB
      LANGUAGE C {
      #include <string.h>
          // store number of bounds
          size_t *num_bounds = malloc(aggr_group.count * sizeof(size_t));
          size_t *bounds_cur_sizes = malloc(aggr_group.count * sizeof(size_t));
          memset(num_bounds, 0, aggr_group.count * sizeof(size_t));
          memset(bounds_cur_sizes, 0, aggr_group.count * sizeof(size_t));
          // initialize one aggregate per group
          result->initialize(result, aggr_group.count);
          // zero initialize the sums
          for(size_t i = 0; i < result->count; i++)
          {
              bounds_cur_sizes[i] = 16; // 16 elements first
              result->data[i].data = malloc(bounds_cur_sizes[i] * sizeof(int64_t));
          }

          // collect bounds for each of the groups
          for(size_t i = 0; i < input.count; i++)
          {
              int grp = aggr_group.data[i];
              size_t cur_pos = num_bounds[grp];
              if (cur_pos >= bounds_cur_sizes[grp])
              {
                  size_t *old_bounds = result->data[grp].data;
                  result->data[grp].data = malloc(bounds_cur_sizes[grp] * sizeof(int64_t) * 2);
                  memcpy(result->data[grp].data, old_bounds, bounds_cur_sizes[grp] * sizeof(int64_t));
                  bounds_cur_sizes[grp] *= 2;
              }

              num_bounds[grp]++;
              result->data[grp].size += sizeof(int64_t);
              ((int64_t *) result->data[grp].data)[cur_pos] = input.data[i];
          }
      };
