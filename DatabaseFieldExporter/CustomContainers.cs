using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace NZ01
{

    /// Classes for customised containers

    /// <summary>
    /// Generic class for thread-safe access to a Dictionary of HashSets
    /// </summary>
    /// <typeparam name="T">Key Type T</typeparam>
    /// <typeparam name="U">Value Type U</typeparam>
    /// <remarks>
    /// All access is controlled by a locker so that any operation on the container
    /// is isolated and thread safe.
    /// HashSet of Type U is used to ensure each item is unique.
    /// </remarks>
    public class DictionaryOfHashSets<T, U> : IEnumerable<T>
    {
        /////////////////////
        // INSTANCE MEMBERS

        private Dictionary<T, HashSet<U>> _dic;


        // CTOR
        public DictionaryOfHashSets()
        {
            _dic = new Dictionary<T, HashSet<U>>();
        }


        /// <summary>
        /// Copy Constructor
        /// </summary>
        /// <param name="origDicOfHSets">Item to copy</param>
        /// <remarks>
        /// There is a requirement to copy a DictionaryOfHashSets such that
        /// changes can be made to the copied object without effecting the 
        /// original object.  To do this, the dictionary needs to maintain
        /// it's own HashSets, so these must be copied.  The items in the 
        /// HashSets will be copied by reference still, so a change to an
        /// item in the HashSet in the copy will effect the original.
        /// If we were to make a copy of just the dictionary, each item in
        /// the dictionary (HashSet of Type U) would just be a reference
        /// to the original HashSet, and there would be no isolation.
        /// </remarks>
        public DictionaryOfHashSets(DictionaryOfHashSets<T, U> origDicOfHSets)
        {
            // Iterate the original dictionary, and make a new HashSet
            // at each key.
            foreach (KeyValuePair<T, HashSet<U>> kvp in origDicOfHSets._dic)
            {
                var t = kvp.Key;
                var hsetu = kvp.Value;

                var copyOfHsetU = new HashSet<U>(hsetu);
                _dic.Add(t, copyOfHsetU);
            }
        }




        /////////////////////
        // MEMBER FUNCTIONS


        /// <summary>
        /// Add a value type U object to the set for this key type T object
        /// </summary>
        /// <param name="t">Key type T</param>
        /// <param name="u">Value type U</param>
        /// <returns>bool, true implies successful addition</returns>
        public bool Add(T t, U u)
        {
            HashSet<U> hset = null;
            bool success = _dic.TryGetValue(t, out hset);
            if (success)
            {
                return hset.Add(u); // Hash set exists; Add this u to the set
            }
            else
            {
                hset = new HashSet<U>();
                bool successSetAdd = hset.Add(u);
                _dic.Add(t, hset);
                return successSetAdd;
            }
        }


        /// <summary>
        /// Remove a value type U object for all key Type T objects
        /// </summary>
        /// <param name="u">Value type U</param>
        /// <returns>bool, true implies successful removal</returns>
        public bool Remove(U u)
        {
            bool removed = false;

            HashSet<T> TsWithEmptyHashSets = new HashSet<T>();

            foreach (KeyValuePair<T, HashSet<U>> kvp in _dic)
            {
                T t = kvp.Key;
                HashSet<U> hset = kvp.Value;

                // If the u is removed from any hash set, consider this a success.
                if (hset.Remove(u))
                    removed = true;

                if (!hset.Any())
                    TsWithEmptyHashSets.Add(t);
            }

            // Remove any empty containers
            foreach (T t in TsWithEmptyHashSets)
            {
                _dic.Remove(t);
            }

            return removed;
        }


        /// <summary>
        /// Remove a value type U object for a specific key Type T object
        /// </summary>
        /// <param name="t">Key Type T</param>
        /// <param name="u">Value type U</param>
        /// <returns>bool; true implies successful removal</returns>
        public bool Remove(T t, U u)
        {
            bool removed = false;

            HashSet<U> hset = null;
            bool success = _dic.TryGetValue(t, out hset);
            if (success)
            {
                removed = hset.Remove(u);
                if (!hset.Any())
                {
                    _dic.Remove(t);
                }
                return removed;
            }

            return removed;
        }


        /// <summary>
        /// Get the number of items in the HashSet at this key
        /// </summary>
        /// <param name="t">Key Type T</param>
        /// <returns>int; Count of items of Type U in HashSet</returns>
        public int Count(T t)
        {
            int count = 0;

            HashSet<U> hset = null;
            bool success = _dic.TryGetValue(t, out hset);
            if (success)
            {
                return hset.Count();
            }

            return count;
        }

        /// <summary>
        /// Retrieve the set of value type U objects for the key type T object
        /// </summary>
        /// <param name="t">Key Type T</param>
        /// <returns>Container of type U</returns>
        /// <remarks>
        /// </remarks>
        public List<U> Get(T t)
        {
            List<U> us = new List<U>();

            HashSet<U> hset = null;
            bool success = _dic.TryGetValue(t, out hset);
            if (success)
            {
                us = hset.ToList();
            }

            return us;
        }


        /// <summary>
        /// Return every contained value object.
        /// </summary>
        /// <returns>
        /// List of Type U; Guaranteed no duplicates.
        /// </returns>
        public List<U> GetAllContainedValues()
        {
            HashSet<U> usUnique = new HashSet<U>();

            foreach (KeyValuePair<T, HashSet<U>> kvp in _dic)
            {
                HashSet<U> us = kvp.Value;
                foreach (U u in us)
                {
                    usUnique.Add(u);
                }
            }

            return usUnique.ToList();
        }

        /// <summary>
        /// Clear the set of subscribed value Type U objects for a key type T object
        /// </summary>
        /// <param name="t">Key Type T</param>
        /// <returns>bool; true implies the key object t existed and its set was cleared.</returns>
        public bool Clear(T t)
        {
            bool cleared = false;

            if (_dic.ContainsKey(t))
            {
                cleared = _dic.Remove(t);
            }

            return cleared;
        }


        /// <summary>
        /// Clear all of the subscription sets
        /// </summary>
        public void Clear()
        {
            _dic.Clear();
        }


        /// <summary>
        /// Check if Value is contained in the HashSet held at this Key
        /// </summary>
        /// <param name="u">Value Type U</param>
        /// <param name="t">Key Type T</param>
        /// <returns>bool; true if value is in HashSet, false if key is not present or value is not in HashSet</returns>
        public bool ContainsValueAtKey(U u, T t)
        {
            if (_dic.ContainsKey(t))
            {
                //HashSet<U> hset = _dic[t];
                return _dic[t].Contains(u);
            }

            return false;
        }

        ////////////////////////////////////////////////
        // START BOILER PLATE TO IMPLEMENT IENUMERABLE

        public IEnumerator<T> GetEnumerator()
        {
            foreach (KeyValuePair<T, HashSet<U>> kvp in _dic)
            {
                yield return kvp.Key;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        // FINISH BOILER PLATE TO IMPLEMENT IENUMERABLE
        /////////////////////////////////////////////////


    } // end of class DictionaryOfHashSets<T,U>

} // end of namespace NZ01