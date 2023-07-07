from __future__ import unicode_literals, print_function
import pandas as pd
import plac
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
import re
#import nltk
import spacy
import os
#from nltk.corpus import words
#from nltk.corpus import stopwords
from tqdm import tqdm

# nltk.download('words')
#stopwords = set(stopwords.words('english'))

replacecorp = pd.read_csv('Data/replace-corpus-final.csv')
nlp = spacy.load('Models/swift_nlp_model_v003', disable=['tokenizer'])

def process(DATA):
  
    DATA.columns = ['index', 'to_match']
    index = DATA['index']
    TEST_DATA = DATA['to_match']
    TEST_DATA = pd.Series(TEST_DATA).str.lower()
    TEST_DATA = TEST_DATA.str.replace('[^0-9A-Za-z\s]', '')
    countries = ['afghanistan', 'aland islands', 'albania', 'algeria', 'algerie', 'american samoa', 'andorra', 'africa',
             'angola', 'anguilla', 'antarctica', 'antigua and barbuda', 'argentina', 'armenia', 'aruba', 
             'australia', 'austria', 'azerbaijan', 'bahamas', 'bahrain', 'bangladesh', 'barbados', 'belarus', 
             'belgium', 'belize', 'benin', 'bermuda', 'bhutan', 'bolivia', 'bonaire', 'bosnia and herzegovina', 
             'botswana', 'bouvet island', 'brazil', 'british indian ocean territory', 'brunei darussalam', 'bulgaria', 
             'burkina faso', 'burundi', 'cambodia', 'cameroon', 'canada', 'cape verde', 'cayman islands', 'central african republic', 
             'chad', 'chile', 'china', 'christmas island', 'cocos keeling islands', 'colombia', 'comoros', 'congo', 'congo', 
             'costa rica', "cote divoire", 'croatia', 'cuba', 'curaçao', 'cyprus', 'czech republic', 'denmark', 'djibouti', 
             'dominican republic', 'ecuador', 'egypt', 'el salvador', 'equatorial guinea', 'eritrea', 'estonia', 
             'ethiopia', 'falkland islands', 'faroe islands', 'fiji', 'finland', 'france', 'french guiana', 'french polynesia', 
             'french southern territories', 'gabon', 'gambia', 'georgia', 'germany', 'ghana', 'gibraltar', 'greece', 'greenland', 
             'grenada', 'guadeloupe', 'guam', 'guatemala', 'guernsey', 'guinea', 'guinea-bissau', 'guyana', 'haiti', 
             'heard island and mcdonald islands', 'holy see vatican city state', 'honduras', 'hong kong', 'hungary', 
             'iceland', 'indonesia', 'iran', 'iraq', 'ireland', 'isle of man', 'israel', 'italy', 'jamaica', 
             'japan', 'jersey', 'jordan', 'kazakhstan', 'kenya', 'kiribati', "korea", 'kuwait', 'kyrgyzstan', "laos", 
             'latvia', 'lebanon', 'lesotho', 'liberia', 'libya', 'liechtenstein', 'lithuania', 'luxembourg', 'macao', 
             'macedonia', 'madagascar', 'malawi', 'malaysia', 'maldives', 'mali', 'malta', 'marshall islands', 'martinique', 
             'mauritania', 'mauritius', 'mayotte', 'mexico', 'micronesia', 'moldova', 'monaco', 'mongolia', 'montenegro', 
             'montserrat', 'morocco', 'mozambique', 'myanmar', 'namibia', 'nauru', 'nepal', 'netherlands', 'new caledonia', 
             'new zealand', 'nicaragua', 'nigeria', 'niue', 'norfolk island', 'northern mariana islands', 'norway', 
             'oman', 'pakistan', 'palau', 'palestinian', 'palestine', 'panama', 'papua new guinea', 'paraguay', 
             'peru', 'philippines', 'pitcairn', 'poland', 'portugal', 'puerto rico', 'qatar', 'réunion', 'romania', 
             'russia', 'rwanda', 'saint barthelemy', 'saint helena', 'saint kitts and nevis', 'saint lucia', 
             'saint martin', 'saint pierre and miquelon', 'saint vincent and the grenadines', 'samoa', 'san marino', 
             'sao tome and principe', 'saudi arabia', 'senegal', 'serbia', 'seychelles', 'sierra leone', 'singapore', 
             'sint maarten', 'slovakia', 'slovenia', 'solomon islands', 'somalia', 'south africa', 'south georgia and the south sandwich islands', 'spain', 'sri lanka', 'sudan', 'suriname', 'south sudan', 'svalbard and jan mayen', 'swaziland', 'sweden', 
             'switzerland', 'syrian arab republic', 'taiwan', 'tajikistan', 'tanzania', 'thailand', 'timor-leste', 'togo', 
             'tokelau', 'tonga', 'trinidad and tobago', 'tunisia', 'turkey', 'turkmenistan', 'turks and caicos islands', 'tuvalu', 
             'uganda', 'ukraine', 'united arab emirates', 'united kingdom', 'united states', 'united states minor outlying islands', 
             'uruguay', 'uzbekistan', 'vanuatu', 'venezuela', 'viet nam', 'vietnam', 
             'virgin islands, british', 'virgin islands', 'wallis and futuna', 'yemen', 'zambia', 'zimbabwe', 'uae', 'ksa']
    
    for country in tqdm(countries):
        c = ' '+country+' '
        TEST_DATA = TEST_DATA.str.replace(country, c)
    
    ## SEPARATE NUMBERS FROM LETTERS
    pattern = r'(-?[0-9]+\.?[0-9]*)'
    num_splitted = [] 

    for to_match in tqdm(TEST_DATA):
        split = repr(' '.join(segment for segment in re.split(pattern, to_match) if segment))
        num_splitted.append(split)
        
    tomatch = pd.DataFrame()
    tomatch['to_match'] = num_splitted
    tomatch['orig'] = TEST_DATA
    
    
    
    replacecorp2 = replacecorp[replacecorp['type']=='ext']

    for idx in tqdm(range(len(replacecorp2))):
        w = replacecorp['word'][idx]
#        if w in stopwords:#.words('english'):
#            pass
#        else:
        toreplace = replacecorp['replaceword'][idx]
        tomatch['to_match'] = tomatch['to_match'].str.replace(w, toreplace)

        w1 = w+' er '
        toreplace1 = w+'er '
        tomatch['to_match'] = tomatch['to_match'].str.replace(w1, toreplace1)

        w2 = w+' ing '
        toreplace2 = w+'ing '
        tomatch['to_match'] = tomatch['to_match'].str.replace(w2, toreplace2)

        w3 = w+'er s '
        toreplace3 = w+'ers '
        tomatch['to_match'] = tomatch['to_match'].str.replace(w3, toreplace3)

        w4 = w+' s '
        toreplace4 = w+'s '
        tomatch['to_match'] = tomatch['to_match'].str.replace(w4, toreplace4)

        w4 = w+' ed '
        toreplace4 = w+'ed '
        tomatch['to_match'] = tomatch['to_match'].str.replace(w4, toreplace4)
    
    print('phase 1 done.')
    
    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time = ", current_time)
    
    for i in range(12):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
            
    tomatch['to_match'] = tomatch['to_match'].str.replace('pobox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('po box', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p o box', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p obox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p o b ox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p ob ox', ' pobox ')        
    tomatch['to_match'] = tomatch['to_match'].str.replace('pobo x', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pob ox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p obox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p o box', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('post box', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('postbox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pbbox', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' p o ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' p o b ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('p o b$', ' pobox ', regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace(' po ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' p o$', ' pobox ', regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace(' po bo ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' po bo$', ' pobox ', regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace(' p o bo ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' p o bo$', ' pobox ',regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace(' po b ', ' pobox ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' po b$', ' pobox ',regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace('pt. ', '',regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace('pt.', '',regex=True)
    tomatch['to_match'] = tomatch['to_match'].str.replace('m/s', '',regex=True)
            
    # company extensions
    tomatch['to_match'] = tomatch['to_match'].str.replace('llc', ' llc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('llp', ' llp ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('w llp', ' wllp ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ltd', ' ltd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('limited', ' limited ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('company', ' company ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('compnay', ' company ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('bpk', ' bpk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bvba', ' bvba ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cvoa', ' cvoa ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('eurl', ' eurl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('gbr', ' gbr ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('gcv', ' gcv ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('gesmbh', ' gesmbh ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ges mbh', ' gesmbh ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jtd', ' jtd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kda', ' kda ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kdd', ' kdd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kft', ' kft ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kgaa', ' kgaa ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kkt', ' kkt ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace(' kk ', ' kk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kol srk', ' kol srk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kolsrk', ' kol srk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kom srk', ' kom srk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('komsrk', ' kom srk ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('nv', ' nv ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ldc', ' ldc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('plc', ' plc ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('pma', ' pma ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pmdn', ' pmdn ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ohg', ' ohg ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('pty', ' pty ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('prc', ' prc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('s de rl', ' s de rl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sderl', ' s de rl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sen nc', ' s en nc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sennc', ' s en nc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('safi', ' safi ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sdn bhd', ' sdn bhd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sdnbhd', ' sdn bhd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sgps', ' sgps ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('snc', ' snc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('soparfi', ' soparfi ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('spa ', ' spa ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('srl', ' srl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sprl', ' sprl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sp zoo', ' sp zoo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('spzoo', ' sp zoo ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('bhd', ' bhd ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sarl', ' sarl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('gmbh', ' gmbh ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('limited', ' limited ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('spols ro', ' spol sro ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' spolsro', ' spol sro ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('spol s r o', ' spol sro ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('s polsro', ' spol sro ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('l l c', ' llc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('w l l', ' wll ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('wll', ' wll ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('w ll', ' wll ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('kscc', ' kscc ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('city', ' city ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pte', ' pte ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bscc', ' bscc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sanayi ve', ' san ve')
    tomatch['to_match'] = tomatch['to_match'].str.replace('printing', ' printing ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('hospital', ' hospital ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mfg', ' manufacturing ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('gencont', ' general cont')
    tomatch['to_match'] = tomatch['to_match'].str.replace('home', ' home ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('home s ', ' homes ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ans cull', ' and scull ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('dresserrand', 'dresse rand')
    
    for i in range(12):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
    


    tomatch['to_match'] = tomatch['to_match'].str.replace('inter national', ' international ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('infra structure', ' infrastructure ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ex chang e', ' exchange')
    tomatch['to_match'] = tomatch['to_match'].str.replace('i nv es', ' inves')
    tomatch['to_match'] = tomatch['to_match'].str.replace('e nv iro', ' enviro')
    tomatch['to_match'] = tomatch['to_match'].str.replace('i nv est', 'invest')
    tomatch['to_match'] = tomatch['to_match'].str.replace('e nv ir', 'envir')
    tomatch['to_match'] = tomatch['to_match'].str.replace('o nv er', 'onver')
    tomatch['to_match'] = tomatch['to_match'].str.replace('japan ese', 'japanese ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('north ern', 'northern ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('west ern', 'western ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('east ern', 'eastern ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('south ern', 'southern ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('metal lic', 'metallic ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('drug store', 'drug store ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('culture', 'culture ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('resorts', 'resorts ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fertilizers', 'fertilizers ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fertilisers', 'fertilizers ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ortcite', 'ort cite')
    tomatch['to_match'] = tomatch['to_match'].str.replace('expcite', 'exp cite')
    tomatch['to_match'] = tomatch['to_match'].str.replace('impcite', 'imp cite')
    tomatch['to_match'] = tomatch['to_match'].str.replace('export', ' export ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('export er ', 'exporter ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coibn', 'co ibn')
    tomatch['to_match'] = tomatch['to_match'].str.replace('contracting', ' contracting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('consulting', ' consulting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('algeria', ' algeria ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('trading', ' trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('douar', ' douar ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industrie', ' industrie ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industrie s', ' industries ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industries', ' industries ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industry', ' industry ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('contco', ' cont co ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industrial', ' industrial ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' indl ', ' industrial ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('zone', ' zone ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cooperative', ' cooperative ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coop', ' coop ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coop erat', ' cooperat')
    tomatch['to_match'] = tomatch['to_match'].str.replace('securities', ' securities ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('security', ' security ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemical', ' chemical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemical s', ' chemicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('commercial', ' commercial ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('services', ' services ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('private', ' private ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('construction', ' construction ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmaceuticals', ' pharma ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmaceutical', ' pharma ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmaceutical s', ' pharma ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('stores', ' stores ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('business', ' business ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('international', ' international ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('engineering', ' engineering ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jordan', ' jordan ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jordan ian', ' jordanian ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('drms', ' drms ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('manufacturing', ' manufacturing ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('manufacturer', ' manufacturer ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('manufacturer s', ' manufacturers ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fertilizer', ' fertilizer ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fertiliser', ' fertiliser ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('marketing', ' marketing ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemicals', ' chemicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('polymedic', ' polymedic ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('store', ' store ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemicals', ' chemicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('petro chemicals', 'petrochemicals ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('cite', ' cite ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('new', ' new ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('re new a', 'renewa')
    tomatch['to_match'] = tomatch['to_match'].str.replace('development', ' development ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('import', ' import ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('import er', ' importer ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('import ing', ' importing ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('import s', ' imports ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('salras', ' sal ras ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('beirut', ' beirut ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('partners', ' partners ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('assembly', ' assembly ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('supply', ' supply ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('supplies', ' supplies ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('supplier', ' supplier ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('suppliers', ' suppliers ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('establishment', ' establishment ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('establishment', 'est')
    tomatch['to_match'] = tomatch['to_match'].str.replace('group', ' group ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('grp', ' group ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('established', ' established ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('solutions', ' solutions ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('financial', ' financial ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('agency', ' agency ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('product', ' product ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('product ion', ' production ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('product s', ' products ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('investment', ' investment ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('investment s', 'investments ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('lotissement', ' lotissement ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('packag', ' packag')
    tomatch['to_match'] = tomatch['to_match'].str.replace('univers', ' univers')
    tomatch['to_match'] = tomatch['to_match'].str.replace('scientifique', ' scientifique ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('general', ' general ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('medicament', ' medicament ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('address', ' address ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('granite', ' granite ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('steel', ' steel ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('steel s ', ' steels ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('department', ' department ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('department s ', ' departments ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('department al', ' departmental ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('r oman ia', ' romania ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('r oman', ' roman ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('offshore', ' offshore ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('byader', ' byader ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jalan', ' jalan ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industria', ' industria')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' limited', ' ltd')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' limted', ' ltd')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' lmted', ' ltd')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' tradg', ' trading')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' trad ', ' trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' trdg', ' trading')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' trd ', ' trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('contg', ' contracting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' cont ', ' contracting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' ind ', ' industry ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' trad est', ' trading est')
    tomatch['to_match'] = tomatch['to_match'].str.replace('office', ' office ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('alyaeai', ' alyafai ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('intl', ' international ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('andcattle', ' and cattle ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('co mpa', ' compa')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cojsfz', ' company ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('storage', ' storage ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coating', ' coating ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' company', ' co')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' comp ', ' co')
    tomatch['to_match'] = tomatch['to_match'].str.replace('hong kong', ' hongkong ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jafza', ' jafza ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('aqaba', ' aqaba ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('building', ' building ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('electrical', ' electrical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('irrigation', ' irrigation ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mezzanine', ' mezzanine ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bldg', 'building')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' eng work', ' engineering work')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' eng const', ' engineering const')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' tr co', ' trading co')
    
    for i in range(12):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
        
    tomatch['to_match'] = tomatch['to_match'].str.replace('building mat ', 'building material ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('appliances', ' appliances ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fertilizer s ', ' fertilizers ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemical s', ' chemicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('brothers', ' brothers ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('pivet', ' pivet ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('algerie', ' algerie ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('export s ', ' exports ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('import s ', ' imports ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('export er', ' exporter ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('export ing', ' exporting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('distribution', ' distribution ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('distributor', ' distributor ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('distributor s', ' distributors')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('trade', ' trade ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('trade r ', ' trader')
    tomatch['to_match'] = tomatch['to_match'].str.replace('eneterprise', ' eneterprise ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('contractor', ' contractor ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('contractor s ', ' contractors ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' s ', 's ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('system s', 'systems')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jordan ian', ' jordanian ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('branch', ' branch ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('corp', ' corp ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' corp ora', ' corpora')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' corp  ora', ' corpora')
    tomatch['to_match'] = tomatch['to_match'].str.replace('corporation', ' corporation ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('in corporation', ' incorporation ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' est', ' est ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' est ab', ' estab')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' est ate', ' estate')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' est im', ' estim')
    tomatch['to_match'] = tomatch['to_match'].str.replace('branch', ' branch ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('zoo', ' zoo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('zoo log', ' zoolog')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' inc', ' inc ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' inc orp', ' incorp')
    tomatch['to_match'] = tomatch['to_match'].str.replace('spavia', ' spa via ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('unit', ' unit ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('unit ed', ' united ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('unit y', ' unity ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('lahore', ' lahore ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jakarta', ' jakarta ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coroom', ' co room ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('road', ' road ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('tel[0-9]', ' tel ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('tel [0-9]', ' tel ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fax[0-9]', ' fax ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fax [0-9]', ' fax ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('spa[0-9]', ' spa ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('tbk', ' tbk ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('gresikjl', ' gresik jl ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('egypt ian', ' egyptian ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('enterprise s', ' enterprises ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('enterprises', ' enterprises ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('in corporate d', ' incorporated ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('oyj', ' oyj ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('factory', ' factory ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('manu factory', ' manufactory ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('machines', ' machines ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' coroom ', ' co room')
    tomatch['to_match'] = tomatch['to_match'].str.replace('german y', ' germany ')
    
    tomatch['to_match'] = tomatch['to_match'].str.replace('veticaret', ' ve ticaret ')
    
    tomatch['to_match'] = tomatch['to_match'].str.replace('defence', ' defence ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('impexp', ' imp exp') 
    tomatch['to_match'] = tomatch['to_match'].str.replace(' imp ', ' import ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' exp ', ' export ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('medical', ' medical ')    
    tomatch['to_match'] = tomatch['to_match'].str.replace('strategy', ' strategy ') 
    tomatch['to_match'] = tomatch['to_match'].str.replace('strategic', ' strategic') 
    tomatch['to_match'] = tomatch['to_match'].str.replace('documentation', ' documentation ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mineral water', ' mineral water ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mineral', ' mineral ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('water', ' water ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('electromechanical', ' electromechanical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('superieur', ' superieur ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jeddah', ' jeddah ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pesticide', ' pesticide ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pesticide s', ' pesticides ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('abrou nv ision', ' abroun vision ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('s uppl', ' suppl')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cor oman del', 'coromandel')
    tomatch['to_match'] = tomatch['to_match'].str.replace('holding s', 'holdings')
    tomatch['to_match'] = tomatch['to_match'].str.replace('holding s', 'holdings')
    tomatch['to_match'] = tomatch['to_match'].str.replace('tourism', ' tourism ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('works', ' works ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('hebron', ' hebron ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jawdah', ' jawdah ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('technology', ' technology ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('technologies', ' technologies ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('aircondition', ' aircondition ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('aircondition ing', ' airconditioning ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('damascus', ' damascus ')
#    tomatch['to_match'] = tomatch['to_match'].str.replace('group', ' group ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('group e ', ' groupe ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('project', ' project ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('project s ', ' projects ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('laboratory', ' laboratory ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('equipment', ' equipment ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('equipment s ', ' equipments ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('arabia', ' arabia ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' arabia n ', ' arabian ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('consultants', ' consultants ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('chemiczne', ' chemicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmacceuticals', ' pharmaceuticals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('shenzhen', ' shenzhen ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('endustrisi', ' endustrisi ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('europe', ' europe ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('europe an ', ' european ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('automotive', ' automotive ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' corporation', ' corp')
    tomatch['to_match'] = tomatch['to_match'].str.replace('incorporated', 'inc')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pvt', ' pvt ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fzc', ' fcz ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fzc o', ' fczo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fzc  o', ' fczo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fcz', ' fcz ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fcz o', ' fczo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fcz  o', ' fczo ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('qatar i ', 'qatari ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jebel ', ' jebel ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bank ac', ' bank ac ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' med ', ' medical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' contand ', ' contracting ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('aluminiumco', ' aluminium co ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' cofibre ', ' co fiber ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('i nv ensys', 'invensys')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' me fze', ' middle east fze')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' med ', ' medical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('saintgobain', 'saint gobain ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' indco ', ' industries co ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('refrigeration', ' refrigeration ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('systems', ' systems ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('arabia n ', 'arabian ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('banimarban', 'bani marban')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' gen cont', ' general cont')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' elec n mech ', ' electromechanical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' gen trd ', ' general trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('trading', ' trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' sol em and gen ', ' solutions electromechanical and general ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' tpt and gen ', ' transport and general ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('greenline', 'green line')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' engg', ' engineering ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('transgulf', 'trans gulf')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmaceuticals', 'pharm ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharmaceutical', 'pharm ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('pharm ', 'pharmaceuticals')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' alum ', ' aluminium ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('extraco ', 'extra co ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('aircond', 'air cond')  
    tomatch['to_match'] = tomatch['to_match'].str.replace('refrige', ' refrige')  
    tomatch['to_match'] = tomatch['to_match'].str.replace('technomec', 'techno mec')  
    tomatch['to_match'] = tomatch['to_match'].str.replace('nonwoven', 'non woven')  
    tomatch['to_match'] = tomatch['to_match'].str.replace(' inds ', ' industry ')  
    tomatch['to_match'] = tomatch['to_match'].str.replace(' gen tr ', ' general trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('private', ' pvt ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('advanced', ' advanced ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('soleproprietorship', ' sole proprietorship ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('proprietorship', ' proprietorship ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('transport', ' transport ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('transport ation', ' transportation ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('proteins', ' proteins ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('maintenance', ' maintenance ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' maint ', ' maintenance ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('beneficiary', ' beneficiary ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('alshafar', 'al shafar')
    tomatch['to_match'] = tomatch['to_match'].str.replace('foods', ' foods ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' andsons ', ' and sons ')
    
    tomatch.loc[tomatch['to_match'].str.contains('sun inter'), 'to_match'] = 'sun international'
    tomatch.loc[tomatch['to_match'].str.contains('life pharma'), 'to_match'] = 'life pharmaceuticals'
    tomatch.loc[tomatch['to_match'].str.contains('blue deebaj'), 'to_match'] = 'blue deebaj fzco'
    
    
    for i in range(12):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
        tomatch['to_match'] = tomatch['to_match'].str.strip()
        
        
    tomatch['to_match'] = tomatch['to_match'].str.replace('mechanical', ' mechanical ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('electro mechanical', ' electromechanical')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mechanical s ', ' mechanicals ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('mercantile', ' mercantile ')

    for i in range(12):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
        tomatch['to_match'] = tomatch['to_match'].str.strip()    
    
    # indonesia
#    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('pt', ' pt ')
    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('jl', ' jl ')
#    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('ji', ' ji ')
#    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('cv', ' cv ')
    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('tbk', ' tbk ')
    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('kahuripan', ' kahuripan ')
    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('asri', ' asri ')
    tomatch.loc[tomatch['to_match'].str.contains('indonesia'), 'to_match'] = tomatch['to_match'].str.replace('t ji wi', ' tjiwi ')

    # lebanon
#    tomatch.loc[tomatch['to_match'].str.contains('lebanon'), 'to_match'] = tomatch['to_match'].str.replace('sal', ' sal ')    
    tomatch.loc[tomatch['to_match'].str.contains('lebanon'), 'to_match'] = tomatch['to_match'].str.replace('sarl', ' sarl ')        

    # egypt
#    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('co', ' co ')
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('co mpa', 'compa')
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('co pper', ' copper ')
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('tred co ', 'tredco ')  
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('co nst', 'const')
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('egypt', ' egypt ') 
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('egypt ian', ' egyptian ') 
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('forair', 'for air') 
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('appliance', ' appliance ') 
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('emsarab', 'ems arab') 
    tomatch.loc[tomatch['to_match'].str.contains('egypt'), 'to_match'] = tomatch['to_match'].str.replace('cairo', ' cairo ')

    
    # jordan
#    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace(' est', ' est ')  
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace(' est abl', ' establ')
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace(' est ate', ' estate')
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace(' est ima', ' estima')
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace('salem', ' salem ')
#    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace('coal', ' co al ')
#    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace('general', ' general ')
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace('zarqa', ' zarqa ')
    tomatch.loc[tomatch['to_match'].str.contains('jordan'), 'to_match'] = tomatch['to_match'].str.replace('amman', ' amman ')
    
    # tunis
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('pharmaceutique', ' pharmaceutique ')  
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('pharmaceutique s', ' pharmaceutiques ')  
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('agricoles', ' agricoles ')  
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('tunis', ' tunis')
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('industrie l', ' industriel')
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('group e ', 'groupe ')
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('siphat', ' siphat ')
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('rue', ' rue ')
    tomatch.loc[tomatch['to_match'].str.contains('tunis'), 'to_match'] = tomatch['to_match'].str.replace('societe', ' societe ')

    # algeria
    tomatch.loc[tomatch['to_match'].str.contains('algeria'), 'to_match'] = tomatch['to_match'].str.replace('medicament', ' medicament ')  
    
    # france
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('industrie l', ' industriel')  
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace(' nv ', 'nv')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('grain es ', 'graines ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('chain es ', 'chaines ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('premier es ', 'premieres ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('chain es ', 'chaines ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('etablissement', ' etablissement ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('france', ' france ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('rue', ' rue ')
    tomatch.loc[tomatch['to_match'].str.contains('france'), 'to_match'] = tomatch['to_match'].str.replace('societe', ' societe ')

    # switzerland
    tomatch.loc[tomatch['to_match'].str.contains('switz'), 'to_match'] = tomatch['to_match'].str.replace('switzerland', ' switzerland ')  
    tomatch.loc[tomatch['to_match'].str.contains('switz'), 'to_match'] = tomatch['to_match'].str.replace(' ag', ' ag ')
    tomatch.loc[tomatch['to_match'].str.contains('switz'), 'to_match'] = tomatch['to_match'].str.replace('rue', ' rue ')
    tomatch.loc[tomatch['to_match'].str.contains('switz'), 'to_match'] = tomatch['to_match'].str.replace(' savia ', ' sa via ')

    # pakistan
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('pakistan', ' pakistan ')
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('mills', ' mills ')
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('coroom', 'co room')
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('plot', ' plot ')
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('lahore', ' lahore ')
    tomatch.loc[tomatch['to_match'].str.contains('pakistan'), 'to_match'] = tomatch['to_match'].str.replace('karachi', ' karachi ')
    
    # denmark
    tomatch.loc[tomatch['to_match'].str.contains('denmark'), 'to_match'] = tomatch['to_match'].str.replace(' nv ', 'nv')

    # india
    tomatch.loc[tomatch['to_match'].str.contains('india'), 'to_match'] = tomatch['to_match'].str.replace('india', ' india ')
    tomatch.loc[tomatch['to_match'].str.contains('india'), 'to_match'] = tomatch['to_match'].str.replace('india n ', 'indian ')

    # iraq
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('iraq', ' iraq ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('iraq i ', ' iraqi ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('iraq  i ', ' iraqi ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('bagdhad', ' bagdhad ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('baghdad', ' baghdad ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('drugs', ' drugs ')
    tomatch.loc[tomatch['to_match'].str.contains('iraq'), 'to_match'] = tomatch['to_match'].str.replace('karadah', ' karadah ')
    
    # spain
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace(' sl', ' sl ')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace(' sl u', ' slu ')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace('spain', ' spain ')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace('montaneses', ' montaneses ')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace(' nv ', 'nv')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace('levant ina', 'levantina')
    tomatch.loc[tomatch['to_match'].str.contains('spain'), 'to_match'] = tomatch['to_match'].str.replace('2025slct', '2025 sl ct')

    # portugal
    tomatch.loc[tomatch['to_match'].str.contains('portugal'), 'to_match'] = tomatch['to_match'].str.replace(' lda', ' lda ')

    # brazil
    tomatch.loc[tomatch['to_match'].str.contains('brazil'), 'to_match'] = tomatch['to_match'].str.replace('sao paulo', ' sao paulo ')

    # taiwan
    tomatch.loc[tomatch['to_match'].str.contains('taiwan'), 'to_match'] = tomatch['to_match'].str.replace('incno', ' inc no ')
    tomatch.loc[tomatch['to_match'].str.contains('taiwan'), 'to_match'] = tomatch['to_match'].str.replace('cono', ' co no ')
    tomatch.loc[tomatch['to_match'].str.contains('taiwan'), 'to_match'] = tomatch['to_match'].str.replace('corpno', ' corp no ')

    # kuwait
    tomatch.loc[tomatch['to_match'].str.contains('kuwait'), 'to_match'] = tomatch['to_match'].str.replace('kuwait', ' kuwait ')

    # bahrain
    tomatch.loc[tomatch['to_match'].str.contains('bahrain'), 'to_match'] = tomatch['to_match'].str.replace('bahrain', ' bahrain ')
    tomatch.loc[tomatch['to_match'].str.contains('bahrain'), 'to_match'] = tomatch['to_match'].str.replace('trdandmedsuppl', ' trd and med supply ')

    # malaysia
    tomatch.loc[tomatch['to_match'].str.contains('malaysia'), 'to_match'] = tomatch['to_match'].str.replace('berhad', ' berhad ')

    # palestine
    tomatch.loc[tomatch['to_match'].str.contains('palestine'), 'to_match'] = tomatch['to_match'].str.replace('hebron', ' hebron ')
    tomatch.loc[tomatch['to_match'].str.contains('palestine'), 'to_match'] = tomatch['to_match'].str.replace('palestine', ' palestine ')
    tomatch.loc[tomatch['to_match'].str.contains('palestine'), 'to_match'] = tomatch['to_match'].str.replace('nablus', ' nablus ')

    # greece
    tomatch.loc[tomatch['to_match'].str.contains('greece'), 'to_match'] = tomatch['to_match'].str.replace('s a ', ' sa ')

    # saudi
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace('riyadh', ' riyadh ') 
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace(' est', ' est ')  
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace(' est abl', ' establ')
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace(' est ima', ' estima')
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace('account of', ' account of ')
    tomatch.loc[tomatch['to_match'].str.contains('saudi'), 'to_match'] = tomatch['to_match'].str.replace('ac of', ' ac of ')

    # sweden
    tomatch.loc[tomatch['to_match'].str.contains('sweden'), 'to_match'] = tomatch['to_match'].str.replace(' ab', ' ab ')

    # dubai
    tomatch.loc[(tomatch['to_match'].str.contains('dubai')) | (tomatch['to_match'].str.contains('united arab')), 'to_match'] = tomatch['to_match'].str.replace('fzco', ' fzco ')
    tomatch.loc[(tomatch['to_match'].str.contains('dubai')) | (tomatch['to_match'].str.contains('united arab')), 'to_match'] = tomatch['to_match'].str.replace('dmcc', ' dmcc ')
    tomatch.loc[tomatch['to_match'].str.contains('uae'), 'to_match'] = tomatch['to_match'].str.replace('abudhabi', ' abu dhabi ')
    tomatch.loc[tomatch['to_match'].str.contains('uae'), 'to_match'] = tomatch['to_match'].str.replace('abu dhabi', ' abu dhabi ')
    tomatch.loc[tomatch['to_match'].str.contains('uae'), 'to_match'] = tomatch['to_match'].str.replace('dubai', ' dubai ')

    # germany
    tomatch.loc[tomatch['to_match'].str.contains('germany'), 'to_match'] = tomatch['to_match'].str.replace(' mbh', ' mbh ')
    tomatch.loc[tomatch['to_match'].str.contains('germany'), 'to_match'] = tomatch['to_match'].str.replace('schletter', ' schletter ')
    
    # italy
    tomatch.loc[tomatch['to_match'].str.contains('italy'), 'to_match'] = tomatch['to_match'].str.replace('via', ' via ')
    tomatch.loc[tomatch['to_match'].str.contains('italy'), 'to_match'] = tomatch['to_match'].str.replace('s p a via', ' spa via ')
    tomatch.loc[tomatch['to_match'].str.contains('italy'), 'to_match'] = tomatch['to_match'].str.replace('ceramichespa', ' ceramiche spa ')
    tomatch.loc[tomatch['to_match'].str.contains('italy'), 'to_match'] = tomatch['to_match'].str.replace('spaeuropa', ' spa europa ')
    
    # for common companies
    tomatch.loc[tomatch['to_match'].str.contains('dmsc steel'), 'to_match'] = 'dbmsc steel fzc'
    
    # turkey
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('veticaret', ' ve ticaret ')
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ma muller i', ' mamulleri ')
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('anonimsirketi', ' anonim sirketi ')  
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace(' ticas', ' tic as ')    
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace(' tic as', ' tic as ')      
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace(' tica s', ' tic as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace(' tic a s', ' tic as ')
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticaretas', ' ticaret as ')    
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticaret as', ' ticaret as ')      
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticareta s', ' ticaret as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticaret a s', ' ticaret as ')    
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('santicas', ' san tic as ')
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('santic as', ' san tic as ')
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('fabrikalarias', ' fabrikalari as ')       
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticve sanas', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('tic ve sanas', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticvesanas', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('ticvesan as', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('tic vesanas', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('tic vesan as', ' tic ve san as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace(' sanayias ', ' sanayi as ') 
    tomatch.loc[(tomatch['to_match'].str.contains('turkey')) | (tomatch['to_match'].str.contains('istanbul')), 'to_match'] = tomatch['to_match'].str.replace('istanbul', ' istanbul ')

   # bangladesh
    tomatch.loc[tomatch['to_match'].str.contains('bangladesh'), 'to_match'] = tomatch['to_match'].str.replace('chittagong', ' chittagong ')
    tomatch.loc[tomatch['to_match'].str.contains('bangladesh'), 'to_match'] = tomatch['to_match'].str.replace('bangladesh', ' bangladesh ')
    
    # belgium
    tomatch.loc[tomatch['to_match'].str.contains('belgium'), 'to_match'] = tomatch['to_match'].str.replace(' n v', ' nv ')

    # qatar
    tomatch.loc[tomatch['to_match'].str.contains('qatar'), 'to_match'] = tomatch['to_match'].str.replace('doha', ' doha ')
    tomatch.loc[tomatch['to_match'].str.contains('qatar'), 'to_match'] = tomatch['to_match'].str.replace('qatar', ' qatar ')
    tomatch.loc[tomatch['to_match'].str.contains('qatar'), 'to_match'] = tomatch['to_match'].str.replace('qatar i ', ' qatari ')
    
    ## fixing part
    tomatch['to_match'] = tomatch['to_match'].str.replace('i nv entur', ' inventur ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('larsen toubro', 'larsen and toubro')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industry llc industry llc', 'industry llc')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' const ', ' construction ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('industries industries', 'industries')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' andtr ', ' and trading ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' tr co', ' trading co')
    tomatch['to_match'] = tomatch['to_match'].str.replace('qatar ia ', 'qataria ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('alfaisal drug', 'alfaiasel drug')
    tomatch['to_match'] = tomatch['to_match'].str.replace('scientific', ' scientific ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bureau', ' bureau ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('boulevard', ' boulevard ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cofor', ' co for ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('althuraya', 'althuraya ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('coalmuq', ' co almuq')
    tomatch['to_match'] = tomatch['to_match'].str.replace('forces', ' forces ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('rayyan', ' rayyan ')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' trans ', ' transport ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('fitness', ' fitness ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('appliance', ' appliance ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('global', ' global ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('bouard', ' bouard ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ciepa', ' ciepa ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('belhorizon', ' bel horizon ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cevitalspa', ' cevital spa ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('nouveau', ' nouveau ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('a nv erally', 'anverally')
    tomatch['to_match'] = tomatch['to_match'].str.replace('forces drms', 'force directorate of the royal medical services')
    tomatch['to_match'] = tomatch['to_match'].str.replace('jordam armed', ' jordan armed ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('theghq', 'ghq ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cattlewasfi', ' cattle wasfi ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('cattle wasfi', ' cattle co wasfi ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ldaquinta', 'lda quinta')
    tomatch['to_match'] = tomatch['to_match'].str.replace('crop wadi', 'corp wadi')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' co reg', ' co')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' fishal', ' fish al ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('e nv elo', 'envelo')
    tomatch['to_match'] = tomatch['to_match'].str.replace('structurel', ' structurel ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('structural', ' structural ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('readmix co', 'ready mix concrete')
    tomatch['to_match'] = tomatch['to_match'].str.replace('ramaco', 'ramco')
    tomatch['to_match'] = tomatch['to_match'].str.replace('so nv adia', 'sonvadia')
    tomatch['to_match'] = tomatch['to_match'].str.replace('teyseer', ' teyseer ')
    tomatch['to_match'] = tomatch['to_match'].str.replace('qatar factory for fighting', 'qatar factory for fire fighting')
    tomatch['to_match'] = tomatch['to_match'].str.replace('sahara ge cont', 'sahara general cont')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' tr llc', 'trading llc')
    tomatch['to_match'] = tomatch['to_match'].str.replace(' spalot ', ' spa lot ')
    
    for i in range(20):
        tomatch['to_match'] = tomatch['to_match'].str.replace('  ', ' ')
        
        
        
    
    
    
    tomatch['to_match'] = tomatch['to_match'].str.replace("'", '')
    tomatch['to_match'] = tomatch['to_match'].str.lstrip()
    tomatch['to_match'] = tomatch['to_match'].str.lstrip(' ')
    tomatch['to_match'] = tomatch['to_match'].str.rstrip()
    tomatch['to_match'] = tomatch['to_match'].str.rstrip(' ')
    # tomatch['orig'] = orig
    tomatch = tomatch[['orig', 'to_match']]
    tomatch['index'] = DATA['index']
    tomatch['raw'] = DATA['to_match']
    
    print('phase 2 done.')
    now = datetime.now()

    current_time = now.strftime("%H:%M:%S")
    print("Current Time = ", current_time)
    
    # remove first word if number
    a = tomatch['to_match'].str.split()
    
    def Extract(lst):
        try:
            frst = lst[0]
        except IndexError:
            frst = ''
        return frst
    
    b = []
    for i in a:
        b.append(Extract(i))
    
    tomatch['firstword'] = pd.Series(b)
    tomatch['len'] = tomatch['firstword'].str.len()
    tomatch['numcheck'] = tomatch['firstword'].str.isdigit()
    
    tomatch.loc[(tomatch['numcheck']==True) & (tomatch['len']>3), 'to_match'] = tomatch['to_match'].str.replace('[0-9]','')

    tomatch['to_match'] = tomatch['to_match'].str.lstrip()
    tomatch['to_match'] = tomatch['to_match'].str.lstrip(' ')
    tomatch['to_match'] = tomatch['to_match'].str.rstrip()
    tomatch['to_match'] = tomatch['to_match'].str.rstrip(' ')
    
    print(tomatch.info())
    return tomatch
    
    
def predict(tomatch):
    org = []
    loc = []
    indexno = []
    orig1 = []
    orig2 = []
    print(f'predicting {len(tomatch)} number of rows...')

    for i in tqdm(range(len(tomatch))):
        text = tomatch['to_match'][i]
        indx = tomatch['index'][i]
        doc = nlp(text)
            
        try:
            if doc.ents[0].label_=='ORG':
                org.append(doc.ents[0].text)
                try:
                    loc.append(doc.ents[1].text)
                except:
                    loc.append('no loc')
            if doc.ents[0].label_=='LOC':
                org.append('no org')
                loc.append(doc.ents[0].text)
        except IndexError:
            org.append('no org')
            loc.append('no loc')            
        except:
            org.append('error found while extracting ORG')
            loc.append('error found while extracting LOC')  
            print(f'error found in row {indx}')
              
        orig1.append(text)
        indexno.append(indx)
        
    neworg = pd.Series(org)
    neworg = neworg.str.replace("'", '')
    neworg = neworg.str.replace('  ', ' ')
    newloc = pd.Series(loc)
    newloc = newloc.str.replace("'", '')
    newloc = newloc.str.replace('  ', ' ')
    orgtest_result = pd.DataFrame()
    orgtest_result['original'] = orig1
    orgtest_result['org_detected'] = neworg
    orgtest_result['loc_detected'] = newloc
    orgtest_result['indexno'] = indexno
    orgtest_result['org_cleaned'] = neworg

    orgtest_result['org_cleaned'] = orgtest_result['org_cleaned'].str.title()
    orgtest_result['original'] = orgtest_result['original'].str.replace("'", '')
    
    
    #     orgtest_result['org_cleaned'] = orgtest_result['org_cleaned'].str.replace(' Llp ', ' LLP ')
    #     orgtest_result['org_cleaned'] = orgtest_result['org_cleaned'].str.replace(' Llc ', ' LLC ')
    #     orgtest_result['org_cleaned'] = orgtest_result['org_cleaned'].str.replace(' Bscc ', ' Bsc (c) ')
    #     orgtest_result.loc[orgtest_result['org_cleaned'].str.endswith(' As'), 'org_cleaned'] = orgtest_result['org_cleaned'].str.replace(' As', 'AS')

    return orgtest_result
