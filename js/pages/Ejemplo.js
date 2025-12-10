import { userService } from '../api/user.service.js';

let modalInstance = null; // Guardará la instancia del modal de Bootstrap
let originalMail = null;
let createModalInstance = null;

function createUserRow(user) {
  const statusBadge = user.estado 
    ? `<span class="badge bg-success">Activo</span>`
    : `<span class="badge bg-danger">Inactivo</span>`;

  const userId = user.id_usuario;

  return `
    <tr>
      <td class="px-0">
        <div class="d-flex align-items-center">
          <img src="./assets/images/profile/user-3.jpg" class="rounded-circle" width="40" alt="flexy" />
          <div class="ms-3">
            <h6 class="mb-0 fw-bolder">${user.nombre_completo}</h6>
            <span class="text-muted">${user.num_documento}</span>
          </div>
        </div>
      </td>
      <td class="px-0">${user.correo}</td>
      <td class="px-0">${user.rol}</td>
      <td class="px-0">
        <div class="form-check form-switch ms-2 d-inline-block">
            <input class="form-check-input user-status-switch" type="checkbox" role="switch" 
                   id="switch-${userId}" data-user-id="${userId}" 
                   ${user.estado ? 'checked' : ''}>
            <label class="form-check-label" for="switch-${userId}">
              ${user.estado ? 'Activo' : 'Inactivo'}
            </label>
        </div>
      </td>
      <td class="px-0 text-dark fw-medium text-end">${user.nombre_rol}</td>
      <td class="px-0 text-end">
          <button class="btn btn-sm btn-info btn-edit-user" data-user-email="${user.email}"><i class="fa-regular fa-pen-to-square"></i></button>
      </td>
    </tr>
  `;
}


// --- LÓGICA DE MODAL ---

async function openEditModal(correo) {
  const modalElement = document.getElementById('edit-user-modal');
  if (!modalInstance) {
    modalInstance = new bootstrap.Modal(modalElement);
  }

  try {
    const user = await userService.getUserByEmail(correo);
    originalMail = user.correo;
    document.getElementById('edit-user-id').value = user.id_usuario;
    document.getElementById('edit-nombre_completo').value = user.nombre;
    document.getElementById('edit-correo').value = user.email;
    document.getElementById('edit-telefono').value = user.telefono;
    document.getElementById('edit-documento').value = user.documento;
    modalInstance.show();
  } catch (error) {
    console.error(`Error al obtener datos del usuario ${userId}:`, error);
    alert('No se pudieron cargar los datos del usuario.');
  }
}

// --- MANEJADORES DE EVENTOS ---

async function handleUpdateSubmit(event) {
  event.preventDefault();

  const userId = document.getElementById('edit-user-id').value;
  console.log(userId);
  const updatedData = {
    nombre: document.getElementById('edit-nombre_completo').value,
    telefono: document.getElementById('edit-telefono').value,
    documento: document.getElementById('edit-documento').value,
  };

  let newEmail = document.getElementById('edit-correo').value;

  if (newEmail != originalMail){
    updatedData.email = newEmail;
  }

  try {
    await userService.updateUser(userId, updatedData);
    modalInstance.hide();
    init(); // Recargamos la tabla para ver los cambios
  } catch (error) {
    console.error(`Error al actualizar el usuario ${userId}:`, error);
    alert('No se pudo actualizar el usuario.');
  }
}

async function handleTableClick(event) {
  // Manejador para el botón de editar
  const editButton = event.target.closest('.btn-edit-user');
  if (editButton) {
    const email = editButton.dataset.userEmail;
    console.log(email);
    openEditModal(email);
    return;
  }
}


async function handleStatusSwitch(event) {
  const switchElement = event.target;
  if (!switchElement.classList.contains('user-status-switch')) return;

  const userId = switchElement.dataset.userId;
  const newStatus = switchElement.checked;
  const actionText = newStatus ? 'activar' : 'desactivar';
  
  console.log(newStatus);

  if (confirm(`¿Estás seguro de que deseas ${actionText} este usuario?`)) {
    try {
      await userService.changueEstatusUser(userId, newStatus); // Esta función maneja el cambio de estado
      alert(`El usuario ha sido ${newStatus ? 'activado' : 'desactivado'} exitosamente.`);
      init(); // Recargamos la tabla para ver los cambios
    } catch (error) {
      console.error(`Error al ${actionText} el usuario ${userId}:`, error);
      alert(`No se pudo ${actionText} el usuario.`);
      // Revertir el switch si hay error
      switchElement.checked = !newStatus;
    }
  } else {
    // Revertir el switch si el usuario cancela
    switchElement.checked = !newStatus;
  }
}


// manejador de formulario crear usuarios
async function handleCreateSubmit(event) {
  event.preventDefault();

  let rol = parseInt(document.getElementById('create-id_rol').value);
  console.log(rol);

  const newUserData = {
    nombre: document.getElementById('create-nombre_completo').value,
    documento: document.getElementById('create-identificacion').value,
    email: document.getElementById('create-correo').value,
    pass_hash: document.getElementById('create-password').value,
    telefono: document.getElementById('create-telefono').value,
    id_rol: parseInt(document.getElementById('create-id_rol').value),
    estado: true // Por defecto, los usuarios se crean activos
  };

  try {
    await userService.createUser(newUserData);
    if(createModalInstance) createModalInstance.hide();
    document.getElementById('create-user-form').reset(); // Limpiamos el formulario
    alert('Usuario creado exitosamente.');
    init(); // Recargamos la tabla para ver el nuevo usuario
  } catch (error) {
    console.error('Error al crear el usuario:', error);
    alert('No se pudo crear el usuario.');
  }
}


// --- FUNCIÓN PRINCIPAL DE INICIALIZACIÓN ---

async function init() {
  const tableBody = document.getElementById('users-table-body');
  if (!tableBody) return;

  tableBody.innerHTML = '<tr><td colspan="6" class="text-center">Cargando usuarios ... </td></tr>'; // ✅ CORRECCIÓN: colspan="6"

  try {
    const users = await userService.getUsers();
    if (users && users.length > 0) {
      tableBody.innerHTML = users.map(createUserRow).join('');
    } else {
      tableBody.innerHTML = '<tr><td colspan="6" class="text-center">No se encontraron usuarios.</td></tr>'; // ✅ CORRECCIÓN: colspan="6"
    }
  } catch (error) {
    console.error('Error al obtener los usuarios:', error);
    tableBody.innerHTML = `<tr><td colspan="6" class="text-center text-danger">Error al cargar los datos.</td></tr>`; // ✅ CORRECCIÓN: colspan="6"
  }

  // Aplicamos el patrón remove/add para evitar listeners duplicados
  const editForm = document.getElementById('edit-user-form');
  const createForm = document.getElementById('create-user-form');
  tableBody.removeEventListener('click', handleTableClick);
  tableBody.addEventListener('click', handleTableClick);
  tableBody.removeEventListener('change', handleStatusSwitch);
  tableBody.addEventListener('change', handleStatusSwitch);
  editForm.removeEventListener('submit', handleUpdateSubmit);
  editForm.addEventListener('submit', handleUpdateSubmit);
  createForm.removeEventListener('submit', handleCreateSubmit);
  createForm.addEventListener('submit', handleCreateSubmit);

}

export { init };