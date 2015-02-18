using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

/// <summary>
/// Helper class to output members/properties to string for diagnostics
/// </summary>
namespace FC
{
    public static class FCGenerics
    {

        // Swap two references of type T
        public static void Swap<T>(ref T a, ref T b)
        {
            T tTemp = a;
            a = b;
            b = tTemp;
        }

        public static string ToString<T>(T t,bool bStatic = false)
        {
            Type myType = t.GetType();
            StringBuilder sbReturn = new StringBuilder();
            sbReturn.Capacity = 511;
            sbReturn.Append("[");

            int i = 0;
            while (myType != typeof(object))
            {
                FieldInfo[] fields;
                if ( bStatic == true )
                    fields = FCGenerics.GetTypesStaticFieldInfo(myType);
                else
                    fields = FCGenerics.GetTypesInstanceFieldInfo(myType);

                foreach (FieldInfo field in fields)
                {
                    if (i++ != 0)
                        sbReturn.Append("; ");

                    sbReturn.Append(field.Name);
                    sbReturn.Append(":");
                    sbReturn.Append(field.GetValue(t));
                }

                myType = myType.BaseType;
            }

            sbReturn.Append("]");

            return sbReturn.ToString();
        }

        ////////////////////////////
        // Helpers for ToString<T>
        public static FieldInfo[] GetTypesInstanceFieldInfo(Type type)
        { return type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic); }

        public static FieldInfo[] GetTypesStaticFieldInfo(Type type)
        { return type.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic); }
        //
        ////////////////////////////

    } // end of class FCGenerics

} // end of namespace FC
