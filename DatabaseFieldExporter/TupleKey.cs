using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace NZ01
{

    /// <summary>
    /// Tuple2Key: Tuple of two types to use as a dictionary key
    /// </summary>
    /// <remarks>
    /// Class is a wrapper for a struct.
    /// Taken from: 
    /// http://deanchalk.com/c-creating-reliable-complex-dictionary-keys-using-generic-tuples/
    /// Added own ToString over-ride for diags/dumps.
    /// </remarks>
    public static class Tuple2Key
    {
        public static Tuple2Key<T, U> CreateNew<T, U>(T first, U second)
        {
            return new Tuple2Key<T, U>(first, second);
        }
    }

    /// <summary>
    /// Tuple2Key Struct:
    /// </summary>
    /// <typeparam name="T">First Type</typeparam>
    /// <typeparam name="U">Second Type</typeparam>
    public struct Tuple2Key<T, U> : IEquatable<Tuple2Key<T, U>>
    {
        public readonly T First;
        public readonly U Second;

        public Tuple2Key(T first, U second)
        {
            First = first;
            Second = second;
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;
            if (this.GetType() != obj.GetType())
                return false;
            return AreEqual(this, (Tuple2Key<T, U>)obj);
        }

        public bool Equals(Tuple2Key<T, U> other)
        {
            return AreEqual(this, other);
        }

        private static bool AreEqual(Tuple2Key<T, U> a, Tuple2Key<T, U> b)
        {
            if (!a.First.Equals(b.First))
                return false;
            if (!a.Second.Equals(b.Second))
                return false;
            return true;
        }

        public static bool operator ==(Tuple2Key<T, U> a, Tuple2Key<T, U> b)
        {
            return AreEqual(a, b);
        }

        public static bool operator !=(Tuple2Key<T, U> a, Tuple2Key<T, U> b)
        {
            return !AreEqual(a, b);
        }

        public override int GetHashCode()
        {
            return First.GetHashCode() ^ Second.GetHashCode();
        }

        public override string ToString()
        {
            return "1:" + First.ToString() + ", 2:" + Second.ToString();
        }

    }







    /// <summary>
    /// Tuple3Key - As per Tuple2Key, for 3 items.
    /// </summary>
    public static class Tuple3Key
    {
        public static Tuple3Key<T, U, V> CreateNew<T, U, V>(T first, U second, V third)
        {
            return new Tuple3Key<T, U, V>(first, second, third);
        }
    }

    public struct Tuple3Key<T, U, V> : IEquatable<Tuple3Key<T, U, V>>
    {
        public readonly T First;
        public readonly U Second;
        public readonly V Third;

        public Tuple3Key(T first, U second, V third)
        {
            First = first;
            Second = second;
            Third = third;
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;
            if (this.GetType() != obj.GetType())
                return false;
            return AreEqual(this, (Tuple3Key<T, U, V>)obj);
        }

        public bool Equals(Tuple3Key<T, U, V> other)
        {
            return AreEqual(this, other);
        }

        private static bool AreEqual(
           Tuple3Key<T, U, V> a,
           Tuple3Key<T, U, V> b)
        {
            if (!a.First.Equals(b.First))
                return false;
            if (!a.Second.Equals(b.Second))
                return false;
            if (!a.Third.Equals(b.Third))
                return false;
            return true;
        }

        public static bool operator ==(Tuple3Key<T, U, V> a, Tuple3Key<T, U, V> b)
        {
            return AreEqual(a, b);
        }

        public static bool operator !=(Tuple3Key<T, U, V> a, Tuple3Key<T, U, V> b)
        {
            return !AreEqual(a, b);
        }

        public override int GetHashCode()
        {
            return First.GetHashCode() ^ Second.GetHashCode() ^ Third.GetHashCode();
        }

        public override string ToString()
        {
            return "1:" + First.ToString() + ", 2:" + Second.ToString() + ", 3:" + Third.ToString();
        }
    
    } // end of "public struct Tuple3Key<T, U, V>"


} // end of namespace NZ01