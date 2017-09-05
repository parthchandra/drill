/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#if defined(IS_SSL_ENABLED)

#include "openssl/x509.h"

#if defined _WIN32  || defined _WIN64

#include <stdio.h>
#include <windows.h>
#include <wincrypt.h>
#include <cryptuiapi.h>
#include <iostream>
#include <tchar.h>


#pragma comment (lib, "crypt32.lib")
#pragma comment (lib, "cryptui.lib")

#define MY_ENCODING_TYPE  (PKCS_7_ASN_ENCODING | X509_ASN_ENCODING)

inline
int loadSystemTrustStore(const SSL *ssl) {
    HCERTSTORE hStore;
    PCCERT_CONTEXT pContext = NULL;
    X509 *x509;
    //X509_STORE *store = X509_STORE_new();
     
    SSL_CTX * ctx = SSL_get_SSL_CTX(ssl);
    X509_STORE *store = SSL_CTX_get_cert_store(ctx);

    hStore = CertOpenSystemStore(NULL, L"ROOT");

    if (!hStore)
        return 1;

    while (pContext = CertEnumCertificatesInStore(hStore, pContext)) {
        //uncomment the line below if you want to see the certificates as pop ups
        //CryptUIDlgViewContext(CERT_STORE_CERTIFICATE_CONTEXT, pContext,   NULL, NULL, 0, NULL);

        x509 = NULL;
        x509 = d2i_X509(NULL, (const unsigned char **)&pContext->pbCertEncoded, pContext->cbCertEncoded);
        if (x509) {
            int i = X509_STORE_add_cert(store, x509);

            if (i == 1)
                std::cout << "certificate added" << std::endl;

            X509_free(x509);
        }
    }

    CertFreeCertificateContext(pContext);
    CertCloseStore(hStore, 0);
    system("pause");
    return 0;

}

#else // notwindows
inline
int loadSystemTrustStore(const SSL *ssl) {
    return 0;
}

#endif // WIN32 or WIN64

#endif // SSL_ENABLED
