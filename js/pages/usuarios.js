import { userService } from '../api/user.service.js';

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
      <td class="px-0">${user.nombre_rol}</td>
      <td class="px-0">${user.correo}</td>
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
      <td class="px-0 text-dark fw-medium text-end">${user.id_rol}</td>
      <td class="px-0 text-end">
          <button class="btn btn-sm btn-info btn-edit-user" data-user-email="${user.correo}"><i class="bi bi-pencil-square"></i></button>
      </td>
    </tr>
  `;
}

const btnBuscarEmail = document.getElementById('btnBuscarEmail');
const inputBuscarEmail = document.querySelector('inputBuscarEmail');

const btnGuardarUsuario = document.getElementById('btnGuardarUsuario');

btnGuardarUsuario.addEventListener('click', (e) => {
  e.preventDefault(); // Prevenir el envío del formulario
  createUser();
});

// manejador de formulario crear usuarios
async function createUser() {
  let pass1 = document.getElementById('inputPassword').value;
  let pass2 = document.getElementById('inputConfirmPassword').value;
  
  if (pass1 != pass2) {
    alert('Las contraseñas no coinciden.');
  }else{
    alert('Funcionalidad de creación de usuario en desarrollo.');
    const newUserData = {
      nombre_completo: document.getElementById('inputNombre').value,
      id_rol: parseInt(document.getElementById('inputRol').value),
      correo: document.getElementById('inputCorreo').value,
      num_documento: document.getElementById('inputDocumento').value,
      contra_encript: document.getElementById('inputPassword').value,
      estado: true // Por defecto, los usuarios se crean activos
    }
    try {
      await userService.createUser(newUserData);
      document.getElementById('formSaveUser').reset(); // Limpiamos el formulario
      alert('Usuario creado exitosamente.');
      Init(); // Recargamos la tabla para ver el nuevo usuario
    } catch (error) {
        console.error('Error al crear el usuario:', error);
        alert('No se pudo crear el usuario.');
    }

  };
  
}

// Manejar eventos.
async function handleTableClick(event) {
  // Manejador para el botón de editar
  const editButton = event.target.closest('.btn-edit-user');
  if (editButton) {
    const email = editButton.dataset.userEmail;
    console.log(email);
    openEditModal(email);
     return;
  };
};
let modalInstance = null;
async function openEditModal(correo) {
  const modalElement = document.getElementById('editUserModal');
  if (!modalInstance) {
    modalInstance = new bootstrap.Modal(modalElement);
  }

  try {
    const user = await userService.getUserByEmail(correo);
    // originalMail = user.correo;
    document.getElementById('editNombre').value = user.nombre_completo;
    document.getElementById('editCorreo').value = user.correo;
    document.getElementById('editDocumento').value = user.num_documento;
    document.getElementById('editIdUser').value = user.id_usuario;

    const estadoSelect = document.getElementById('editEstado');
    const option = estadoSelect.querySelector(`option[value="${user.estado}"]`);
    if (option) {
      option.selected = true;
    }

    modalInstance.show();
  } catch (error) {
    console.error(`Error al obtener datos del usuario ${userId}:`, error);
    alert('No se pudieron cargar los datos del usuario.');
  }
}

const btnEditarUsuario = document.getElementById('btnEditarUsuario');
btnEditarUsuario.addEventListener('click', (e) => {
  updateUser();
});

async function updateUser(){
  const id_user = document.getElementById('editIdUser').value;
  const userEditData = {
    nombre_completo: document.getElementById('editNombre').value,
    correo: document.getElementById('editCorreo').value,
    num_documento: document.getElementById('editDocumento').value,
    estado: document.getElementById('editEstado').value
  }
  try {
    await userService.updateUser(id_user, userEditData);
    modalInstance.hide();
    Init(); // Recargamos la tabla para ver los cambios
  } catch (error) {
    console.error('Error al actualizar el usuario ${id_user}:', error);
    alert('No se pudo actualizar el usuario.');
  }
}

async function Init() {
  const users = await userService.getUsers();
  console.log("Usuarios cargados:", users);
  const tbody = document.getElementById('users-container');
  users.forEach(user => {
    const fila = createUserRow(user);
    tbody.insertAdjacentHTML('beforeend', fila);
  });

  tbody.removeEventListener('click', handleTableClick); // Evitar múltiples enlaces
  tbody.addEventListener('click', handleTableClick);
}

export { Init }; 