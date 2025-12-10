
import { request } from './apiClient.js';

export const panelService = {
    getHistorico: () => {
        const endpoint = `/historico/obtener-todos`;
        
        let respuesta = request(endpoint);

        return respuesta;
    },
    
};