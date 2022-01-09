import json
import time
from spade import quit_spade
from spade.agent import Agent
from spade.behaviour import OneShotBehaviour, CyclicBehaviour
from spade.message import Message
from spade.template import Template
import pandas as pd
from bs4 import BeautifulSoup
import requests
import ast
import re
import logging

logging.basicConfig(filename='log.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Load the json file with the crendentials
f = open('credentials.json',)
data = json.load(f)

class Extractor(Agent):
    cont_msg = 0
    cont_source = 0
    iberoempleos = ""
    michaelpage = ""
    pagepersonnel = ""
    infoempleo = ""
    primerempleo = ""
    jobtoday = ""
    
    class Extraction_Send_Beh(OneShotBehaviour):
        
        async def on_start(self):
            print("Extractor running")
            if Extractor.cont_source == 0:
                Extractor.cont_source = Extractor.cont_source + 1
                Extractor.michaelpage = self.extract_from_MichaelPage()
                self.data = Extractor.michaelpage
                self.lenguage = 'MichaelPage'
            elif Extractor.cont_source == 1:
                Extractor.cont_source = Extractor.cont_source + 1
                Extractor.iberoempleos = self.extract_from_iberoempleos()
                self.data = Extractor.iberoempleos
                self.lenguage = 'iberoempleos'
            elif Extractor.cont_source == 2:
                Extractor.cont_source = Extractor.cont_source + 1
                Extractor.infoempleo = self.extract_from_infoempleo()
                self.data = Extractor.infoempleo
                self.lenguage = 'infoempleo'
            elif Extractor.cont_source == 3:
                Extractor.cont_source = Extractor.cont_source + 1
                Extractor.pagepersonnel = self.extract_from_pagepersonnel()
                self.data = Extractor.pagepersonnel
                self.lenguage = 'PagePersonnel'
            else:
                print('ERROR, SOURCE NOT VALID')
                self.kill()
                
        async def run(self):
            # Message
            if self.data != None:
                msg = Message(to=data['transform']['username'])     # Instantiate the message
                msg.set_metadata("performative", "inform")     # Set the "inform" FIPA performative
                #More metadata can be added
                msg.set_metadata("language", self.lenguage)
                msg.body = self.data             # Set the message content

                await self.send(msg)
                logging.info('Mensaje enviado de Agente Extractor a Agente Transformer con los datos extraidos de {}'.format(self.lenguage))
                print("Message sent!")
                
                # Update num of messages sent
                Extractor.cont_msg = Extractor.cont_msg + 1
                self.kill()
                
            if Extractor.cont_msg == 4:
                # stop agent from behaviour
                await self.agent.stop()
                
        def extract_from_MichaelPage(self):
            web = 'https://www.michaelpage.es/'
            logging.info('Extraccion de datos de {}'.format(web))
            print("extracting information from "+ str(web))
            data_dic = {'sector':[],'n_jobs':[],'web':[]}

            page = requests.get('https://www.michaelpage.es/')
            html_soup = BeautifulSoup(page.content, 'html.parser')
            
            panel = html_soup.find('div',{'id':'browse-sector'})
            
            for li in panel.find_all('li'):
                link = li.find('a')['href']
                sector = li.find('a').text

                page_inner = requests.get('https://www.michaelpage.es'+link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')

                n_offers = html_soup_inner.find('span',{'class':'total-search no-of-jobs'}).text

                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)
            
            df = pd.DataFrame().from_dict(data_dic)
            return str(data_dic)
        
        def extract_from_primerempleo(self):
            web = 'https://www.primerempleo.com/'
            logging.info('Extraccion de datos de {}'.format(web))
            print(" extracting information from "+ str(web))
            data_dic = {'sector':[],'n_jobs':[], 'web':[]}

            page = requests.get('https://www.primerempleo.com/')
            html_soup = BeautifulSoup(page.content, 'html.parser')

            panel = html_soup.find('div',{'class':'dropdown pull-left'})

            for li in panel.find_all('li'):
                link = li.find('a')['href']
                sector = li.find('a').text
                sector = sector.strip()

                page_inner = requests.get(link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')

                n_offers = len(html_soup_inner.find_all('div',{'class':'col-md-6'}))

                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)
            
            return str(data_dic)
        
        def extract_from_infoempleo(self):
            web = 'https://www.infoempleo.com/'
            logging.info('Extraccion de datos de {}'.format(web))
            print(" extracting information from "+ str(web))
            data_dic = {'sector':[],'n_jobs':[],'web':[]}

            page = requests.get('https://www.infoempleo.com/trabajo-categorias/')
            html_soup = BeautifulSoup(page.content, 'html.parser')
            web = 'https://www.infoempleo.com/'    
            panel = html_soup.find('ul',{'class':'image-pills mt60'})

            for li in panel.find_all('li'):
                link = li.find('a')['href']
                sector = li.find('h2').text

                sector = sector.replace('Trabajo en ', '')

                page_inner = requests.get('https://www.infoempleo.com'+link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')
                
                sub_cont = html_soup_inner.find('div',{'class':'dtable l12-hide'})
                n_offers = sub_cont.contents[1].text
                n_offers = n_offers.split()[3]

                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)
            
            return str(data_dic)
        
        def extract_from_iberoempleos(self):
            web = 'https://www.iberempleos.es/'
            logging.info('Extraccion de datos de {}'.format(web))
            print("extracting information from "+ str(web))
            data_dic = {'sector':[],'n_jobs':[],'web':[]}

            page = requests.get('https://www.iberempleos.es/')
            html_soup = BeautifulSoup(page.content, 'html.parser')
            
            panel = html_soup.find('div',{'id':'tab-0'})

            for li in panel.find_all('div',{'class':'_f box'}):
                link = li.find('a')['href']
                sector = li.find('a').text
                sector = sector.strip()
                page_inner = requests.get('https://www.iberempleos.es'+link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')

                n_offers = html_soup_inner.find('h2',{'class':'_center-xs'}).text
                n_offers = n_offers.split()[0]
                n_offers = n_offers.replace(".","")

                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)
            
            return str(data_dic)
        
        def extract_from_pagepersonnel(self):
            web = 'https://www.pagepersonnel.es/'
            logging.info('Extraccion de datos de {}'.format(web))
            print("extracting information from "+ str(web))
            data_dic = {'sector':[],'n_jobs':[],'web':[]}

            page = requests.get('https://www.pagepersonnel.es/')
            html_soup = BeautifulSoup(page.content, 'html.parser')
            web = 'https://www.pagepersonnel.es/'
            panel = html_soup.find('div',{'id':'browse-sector'})

            for li in panel.find_all('li'):
                link = li.find('a')['href']
                sector = li.find('a').text

                page_inner = requests.get('https://www.pagepersonnel.es'+link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')

                n_offers = html_soup_inner.find('span',{'class':'total-search no-of-jobs'}).text
                
                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)
            
            return str(data_dic)
        
        def extract_from_jobtoday(self):
    
            data_dic = {'sector':[],'n_jobs':[],'web':[]}
            web = 'https://jobtoday.com/'
            logging.info('Extraccion de datos de {}'.format(web))
            print("extracting information from "+ str(web))
            page = requests.get('https://jobtoday.com/es/trabajos')
            html_soup = BeautifulSoup(page.content, 'html.parser')

            panel = html_soup.find('ul',{'class':'jsx-476655755 FeedPage-categoriesList'})
            #print(panel,'\n')

            for li in panel.find_all('li'):
                link = li.find('a')['href']
                sector = li.find('a').text

                page_inner = requests.get(link)
                html_soup_inner = BeautifulSoup(page_inner.content, 'html.parser')

                n_offers = html_soup_inner.find('title').text
                n_offers = n_offers.replace(',','')
                n_offers = n_offers.split()[0]

                data_dic['sector'].append(sector)
                data_dic['n_jobs'].append(int(n_offers))
                data_dic['web'].append(web)

            return str(data_dic)
    
    async def setup(self):
        print("Agent "+str(self.jid)+ " started")
        logging.info('Agente Extractor ha iniciado')
        michaelBeh = self.Extraction_Send_Beh()
        iberoempleoBeh = self.Extraction_Send_Beh()
        pagepersonnelBeh = self.Extraction_Send_Beh()
        infoempleoBeh = self.Extraction_Send_Beh()
        self.add_behaviour(michaelBeh)
        self.add_behaviour(iberoempleoBeh)
        self.add_behaviour(pagepersonnelBeh)
        self.add_behaviour(infoempleoBeh)

# TRANSFORMER AGENT WHICH STRUCTURE THE INFORMATION AND FILTER THE REQUIRED DATA
class Transformer(Agent):
    recv_michaelpage = ""
    recv_infoempleo = ""
    recv_iberempleo = ""
    recv_pagepersonnel = ""
    michaelpage = False
    infoempleo = False
    iberempleo = False
    pagepersonnel = False
    contador = 0
    df = None
    class Transformer_Recv_Beh(CyclicBehaviour):
        
        async def on_start(self):
            print("Receiving data to be filtered")
            logging.info('Inicio del comportamiento del Agente Transformador que recibe los datos extraidos')
            
        async def run(self):
            msg = await self.receive() 
            if msg:
                
                #print("Message received with content: {}".format(msg.body))
                msg_dict = ast.literal_eval(msg.body)
                if msg.get_metadata("language") == "infoempleo":
                    Transformer.recv_infoempleo = pd.DataFrame().from_dict(msg_dict)
                    Transformer.contador = Transformer.contador + 1
                    Transformer.infoempleo = True
                    logging.info('Datos recibidos en Agente Transformador extraidos de {}'.format(msg.get_metadata("language")))
                elif msg.get_metadata("language") == "MichaelPage":
                    Transformer.recv_michaelpage = pd.DataFrame().from_dict(msg_dict)
                    Transformer.contador = Transformer.contador + 1
                    Transformer.michaelpage = True
                    logging.info('Datos recibidos en Agente Transformador extraidos de {}'.format(msg.get_metadata("language")))
                elif msg.get_metadata("language") == "iberoempleos":
                    Transformer.recv_iberempleo = pd.DataFrame().from_dict(msg_dict)
                    Transformer.contador = Transformer.contador + 1
                    Transformer.iberempleo = True
                    logging.info('Datos recibidos en Agente Transformador extraidos de {}'.format(msg.get_metadata("language")))
                elif msg.get_metadata("language") == "PagePersonnel":
                    Transformer.recv_pagepersonnel = pd.DataFrame().from_dict(msg_dict)
                    Transformer.contador = Transformer.contador + 1
                    Transformer.pagepersonnel = True
                    logging.info('Datos recibidos en Agente Transformador extraidos de {}'.format(msg.get_metadata("language")))
                self.kill()
        
        async def on_end(self):
            print("Finishing behaviour to receive")
        
        
    class Filter_Beh(CyclicBehaviour):
            
        async def run(self):
            
            if Transformer.michaelpage and Transformer.pagepersonnel and Transformer.iberempleo and Transformer.infoempleo:
                    
                print("Running Filter Behaviour")
                frames = [Transformer.recv_iberempleo, Transformer.recv_infoempleo, Transformer.recv_pagepersonnel, Transformer.recv_michaelpage]
                Transformer.df = pd.concat(frames)
                df_aux = Transformer.df
                df_aux = self.filter(df_aux)
                Transformer.df = df_aux
                Transformer.df = Transformer.df.reset_index(drop=True)
                logging.info('Datos filtrados')
                self.kill()
            
        async def on_end(self):
            print("Finishing behaviour to filter the data")
            # Start next behaviour
            self.agent.add_behaviour(self.agent.Transformer_Send_Beh())
            
        # Method to filter those rows which column n_jobs has value < 10
        def remove_less_than_10_njobs(self, df):
            # Get names of indexes for which column n_jobs has value < 10
            indexNames = df[ df['n_jobs'] < 10 ].index
            # Delete these row indexes from dataFrame
            df.drop(indexNames , inplace=True)
            # Return filtered dataframe
            return df
        
        # Method to filter those rows which value of column sector has > 20 characters
        def remove_long_sector_names_20(self, df):
            df = df[df['sector'].apply(lambda x: len(x) < 20)]
            # Return filtered dataframe
            return df
        
        # Method to replace special characters from the dataframe
        def remove_special_characters(self, df):
            # Create dictionary with special characters and its replacements
            elements_to_replace_dict = {'á':'a','é':'e','í':'i','ó':'o','ú':'u',',':'', "\"\"": ""}
            df = df.replace({'sector': elements_to_replace_dict}, regex=True)
            # Return resulting dataframe
            return df
        
        # Method to get sectors that appears in two or more sources
        def get_multiple_source(self, df):
            df = df[df.duplicated(['sector'], keep=False)]
            return df
        
        def filter(self, df):
            df = self.remove_less_than_10_njobs(df)
            df = self.remove_long_sector_names_20(df)
            df = self.remove_special_characters(df)
            df = self.get_multiple_source(df)
            return df
    
    class Transformer_Send_Beh(OneShotBehaviour):
        
        async def on_start(self):
            print("Running Sending Behaviour from Transformer Agent")
            logging.info('Inicio del comportamiento del Agente Transformador que envia los datos filtrados')
            #self.language = "Transformed"
            df_dict = Transformer.df.to_dict()
            self.data = str(df_dict)
                
        async def run(self):
            # Message
            if self.data != None:
                msg = Message(to=data['load']['username'])     # Instantiate the message
                msg.set_metadata("performative", "inform")     # Set the "inform" FIPA performative
                
                msg.body = self.data             # Set the message content

                await self.send(msg)
                print("Message sent!")
                
                self.kill()
        
        async def on_end(self):
            print("Finishing behaviour to send transformed data")
            # stop agent from behaviour
            await self.agent.stop()
        

    async def setup(self):
        print("Transformer Agent started")
        logging.info('Agente Transformador iniciado')
        # Msg Template
        template1 = Template()
        template1.set_metadata("performative", "inform")
        template1.set_metadata("language", "jobtoday")
        template2 = Template()
        template2.set_metadata("performative", "inform")
        template2.set_metadata("language", "MichaelPage")
        template3 = Template()
        template3.set_metadata("performative", "inform")
        template3.set_metadata("language", "iberoempleos")
        template4 = Template()
        template4.set_metadata("performative", "inform")
        template4.set_metadata("language", "infoempleo")
        template5 = Template()
        template5.set_metadata("performative", "inform")
        template5.set_metadata("language", "PagePersonnel")
        
        #jobtoday = self.Transformer_Recv_Beh()
        michaelPage = self.Transformer_Recv_Beh()
        iberoempleos = self.Transformer_Recv_Beh()
        infoempleo = self.Transformer_Recv_Beh()
        pagePersonnel = self.Transformer_Recv_Beh()
        filterBeh = self.Filter_Beh()
        

        # Adding the Behaviour with the template will filter all the msg
        self.add_behaviour(michaelPage, template2)
        self.add_behaviour(iberoempleos, template3)
        self.add_behaviour(infoempleo, template4)
        self.add_behaviour(pagePersonnel, template5)
        self.add_behaviour(filterBeh)
             
        
        
class Loader(Agent):
    recv_data = None
    recv_df = None
    df = None
    class Loader_Recv_Beh(CyclicBehaviour):
        
        async def on_start(self):
            print("Loader Receiving Behaviour is running")
            logging.info('Inicio del comportamiento del Agente Cargador que recibe los datos')
            
        async def run(self):
            msg = await self.receive() 
            if msg:

                Loader.recv_data = ast.literal_eval(msg.body)
                Loader.df = pd.DataFrame().from_dict(Loader.recv_data)
                Loader.df = Loader.df.loc[:, ~Loader.df.columns.str.contains('^Unnamed')]
        
                self.kill()
    

            # stop agent from behaviour
            #await self.agent.stop(
        
        async def on_end(self):
            self.agent.add_behaviour(self.agent.Loader_Export_Beh())
            print("Finishing behaviour to receive transformed data")   
    
    class Loader_Export_Beh(OneShotBehaviour):
        
        async def on_start(self):
            print("Loader Exporting Behaviour is running")
            logging.info('Inicio del comportamiento exportador de los datos')
            
        async def run(self):
            Loader.df = Loader.df.reset_index(drop=True)
            Loader.df.to_csv('result.csv', columns=['sector','n_jobs','web'], index = False)
            logging.info('Datos exportados')
            # stop agent from behaviour
            await self.agent.stop()
        
        async def on_end(self):
            print("Finishing behaviour to export transformed data")   

    
    async def setup(self):
        print("Loader Agent started")
        logging.info('Agente Cargador iniciado')
        # Msg Template
        template1 = Template()
        template1.set_metadata("performative", "inform")
        
        receiver_beh = self.Loader_Recv_Beh()
        
        # Adding the Behaviour with the template will filter all the msg
        self.add_behaviour(receiver_beh, template1)

def main():
    # Create the agent
    print("Creating Agents ... ")
    transformer = Transformer(data['transform']['username'], 
                            data['transform']['password'])
    future_transformer = transformer.start()
    future_transformer.result()
    
    extractor = Extractor(data['extract']['username'], 
                            data['extract']['password'])
    extractor.start()
    
    loader = Loader(data['load']['username'], 
                            data['load']['password'])
    future_transformer_loader = loader.start()
    future_transformer_loader.result()
    
    while transformer.is_alive() and loader.is_alive():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            extractor.stop()
            transformer.stop()
            loader.stop()
            logging.exception("Ejecución interrumpida por teclado")
            break
    
    # Stop the agent
    extractor.stop()
    transformer.stop()
    loader.stop()

    # Quit SPADE, optional, clean all the resources
    quit_spade()
    logging.info('FIN DE LA EJECUCION')
    print("Agents finished")

if __name__ == "__main__":
    main()