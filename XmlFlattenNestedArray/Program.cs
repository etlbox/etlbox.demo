using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using System;
using System.Collections.Generic;
using System.Xml.Serialization;

namespace XmlNestedArray
{
    [XmlRoot("customer")]
    public class Customer
    {
        [XmlAttribute("id")]
        public int Id { get; set; }
        [XmlElement("name")]
        public string Name { get; set; }        
        [XmlArray("payment_methods")]
        public List<PaymentMethod> PaymentMethods { get; set; }
    }

    [XmlType("payment_method")]
    public class PaymentMethod
    {
        [XmlElement("type")]
        public string Type { get; set; }
        [XmlElement("number")]
        public string Number { get; set; }
        [XmlElement("validto")]
        public string ValidTo { get; set; }      
    }

    public class PaymentMethodAndCustomer
    {
        /* Elements from Customer */
        public int CustomerId { get; set; }
        public string CustomerName { get; set; }

        /* Elements from PaymentMethod */
        public string PaymentMethodType { get; set; }
        public string PaymentMethodNumber { get; set; }
        public string PaymentMethodValidTo { get; set; }
    }
    public class Program
    {
        static void Main(string[] args)
        {
            XmlSource<Customer> source = new XmlSource<Customer>("XmlInputData.xml", ResourceType.File);
            source.XmlReaderSettings.DtdProcessing = System.Xml.DtdProcessing.Ignore;

            RowMultiplication<Customer, PaymentMethodAndCustomer> multi = new RowMultiplication<Customer, PaymentMethodAndCustomer>();
            multi.MultiplicationFunc = 
                customer =>
                {
                    List<PaymentMethodAndCustomer> result = new List<PaymentMethodAndCustomer>();
                    foreach (PaymentMethod method in customer.PaymentMethods)                 
                    {
                        var methodAndCustomer = new PaymentMethodAndCustomer();
                        /* Repeating data from customer */
                        methodAndCustomer.CustomerId = customer.Id;
                        methodAndCustomer.CustomerName = customer.Name;

                        /* Specific data from payment methods */
                        methodAndCustomer.PaymentMethodType = method.Type;
                        methodAndCustomer.PaymentMethodNumber = method.Number;
                        methodAndCustomer.PaymentMethodValidTo = method.ValidTo;

                        result.Add(methodAndCustomer);
                    };
                    return result;
                };

            MemoryDestination<PaymentMethodAndCustomer> dest = new MemoryDestination<PaymentMethodAndCustomer>();

            source.LinkTo(multi);
            multi.LinkTo(dest);

            Network.Execute(source);

            /* Display data */

            int lastid = 0;
            foreach (var methodAndCustomer in dest.Data)
            {
                if (lastid != methodAndCustomer.CustomerId)
                {
                    Console.WriteLine($"Customer Data: Id {methodAndCustomer.CustomerId}, Name '{methodAndCustomer.CustomerName}'");
                    lastid = methodAndCustomer.CustomerId;
                }
                Console.WriteLine($"   Payment method data: Type '{methodAndCustomer.PaymentMethodType}'," +
                    $" Number '{methodAndCustomer.PaymentMethodNumber}', ValidTo '{methodAndCustomer.PaymentMethodValidTo}'");
            }

            /* Output */
            /* Customer Data: Id 1, Name 'Peter'
                  Payment method data: Type 'Credit Card', Number '1234-5678', ValidTo '01/24'
                  Payment method data: Type 'Wire transfer', Number 'AB12345435', ValidTo ''
               Customer Data: Id 2, Name 'Mary'
                  Payment method data: Type 'Credit Card', Number '4444-5555', ValidTo '12/26'
                  Payment method data: Type 'Wire transfer', Number 'DA1234356', ValidTo ''
                  Payment method data: Type 'PayPal', Number 'mary@paypal.to', ValidTo ''
            */
        }
    }
}
